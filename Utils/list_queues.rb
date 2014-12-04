#!/usr/bin/env ruby

require 'aws-sdk'
require 'yaml'
require 'timers'

def bg_green(str); return "\033[42m#{str}\033[0m" end
def bg_brown(str); return "\033[43m#{str}\033[0m" end

# Prints array of strings with alternating background colors
def print_with_lines(ary)
  ary.each_slice(2){|a,b| puts bg_green(a); puts b if b}
end

def run_this_thang
  # The client interface performs better than twice as fast as using
  # aws-sdk's higher-level interface. This becomes important when there are many
  # queues.
  client = AWS::SQS::Client.new

  info = []
  dots = ''

  @prefixes.each do |prefix|
    client.list_queues(:queue_name_prefix=>prefix).fetch(:queue_urls,[]).each do |q_url|
      # Display a simple spinner since this operation can take a while when there
      # are lots of queues
      dots = dots + '.'
      print "querying#{dots}\r"

      attr = client.get_queue_attributes(
              :queue_url=>q_url,
              :attribute_names=>['ApproximateNumberOfMessages',
                                 'ApproximateNumberOfMessagesNotVisible']
              )[:attributes]
      info << "%-30s %7d, %7d" % [q_url.split('/').last,
                                  attr['ApproximateNumberOfMessages'],
                                  attr['ApproximateNumberOfMessagesNotVisible']]
    end
  end

  system("clear")
  print_with_lines(info)
end

begin
  @prefixes = []
  while !ARGV.empty?
    @prefixes << ARGV.shift
  end
  @prefixes = [''] if @prefixes.empty?

  AWS.config(YAML.load_file('credentials.yml'))

  run_this_thang()
  time_thread = Thread.new do
    timers = Timers::Group.new
    timers.every(60){run_this_thang()}
    loop {timers.wait}
  end

  loop{gets;run_this_thang()}

  time_thread.join()
rescue Interrupt
end
