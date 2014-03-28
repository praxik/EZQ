#!/usr/bin/env ruby

require 'bundler/setup'
require 'yaml'
require 'aws-sdk'

begin
  q = String.new(ARGV[0])
  if q.empty?
    warn 'You must specify a queue name'
    exit 1
  end

  credentials = YAML.load(File.read('credentials.yml'))
  AWS.config(credentials)

  queue = AWS::SQS.new.queues.named(q) 

  # Get each message from the queue using block form that autodeletes.
  puts "\nClearing queue #{q}"
  puts "This may appear to hang for ~20 seconds at the end. Nothing is wrong." 
  puts "That's just the effect of the long-polling wait timeout on the queue."
  puts "Once you stop seeing 'Deleting...' messages, feel free to interrupt with ctrl-c.\n\n"
  queue.poll(:idle_timeout => 2,:batch_size => 10) do |msg|
    Array(msg).each {|item| puts "Deleting message #{item.id}"}
  end
rescue Interrupt
  warn "\nclear_queue aborted."
end
