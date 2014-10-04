#!/usr/bin/env ruby

# Starts N instances of EZQ::Processor. N is pulled from key
# 'number_of_processes' in userdata OR from processor_fan_out.yml. Userdata
# takes precedence.

# This script also pulls the receive_queue_name from userdata and, if present,
# uses that to override the receive_queue_name that is set in
# receive_queue_config.yml.

require 'bundler/setup'
require 'yaml'
require 'parallel'

command = 'ruby processor.rb -c receive_queue_config.yml'
Dir.chdir(File.dirname(__FILE__))

@num = 1
@rec_queue = ''

begin
  puts 'Looking for number of processors to run in userdata...'
  userdata = YAML.load(File.read('userdata.yml'))
  @num = userdata.fetch('number_of_processes',1)
  if @num == 'auto'
    @num = Parallel.processor_count()  
  else
    @num = @num.to_i
  end
  @rec_queue = userdata.fetch('receive_queue_name','')
  puts "Overriding receive queue setting with #{@rec_queue}" if !@rec_queue.empty?
rescue
  puts 'Number of desired processes not found in userdata. Falling back to number in processor_fan_out.yml.'
  if !File.exists?('processor_fan_out.yml')
    warn 'processor_fan_out.yml does not exist. Aborting processor_fan_out.rb'
    exit 1
  end
  y = YAML.load(File.read('processor_fan_out.yml'))
  if !y.kind_of?(Hash)
    warn 'processor_fan_out.yml is formatted incorrectly. Aborting processor_fan_out.rb'
    exit 1
  end
  @num = y['number_of_processes'].to_i
end


pids = []

@num.times do |idx|
  command += " --log ./processor_" + "%02d" % idx + ".log"
  command += " --queue #{@rec_queue}" if !@rec_queue.empty?
  pids << spawn(command)
end

pids.each do |pid| 
  Process.wait pid
end

