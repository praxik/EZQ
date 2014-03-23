#!/usr/bin/env ruby

# This script runs EZQ::Processor in each of the 44 separate deploy subdirs.

require 'yaml'

#command ="echo $PWD"

command = 'ruby processor.rb -c 6k_worker_queue_config.yml'
Dir.chdir(File.dirname(__FILE__))

if !File.exists?('processor_fan_out.yml')
  warn 'processor_fan_out.yml does not exist. Aborting processor_fan_out.rb'
  exit 1
end

y = YAML.load(File.read('processor_fan_out.yml'))

if !y.kind_of?(Hash)
  warn 'processor_fan_out.yml is formatted incorrectly. Aborting processor_fan_out.rb'
  exit 1
end

num = y['number_of_processes'].to_i

pids = []

num.times do |idx|
  command += " --log ./processor_" + "%02d" % idx + ".log"
  #Dir.chdir("%02d" % idx) {pids << spawn(command)}
  # To run directly in place rather than in numbered subdirs, comment
  # out the previous line and uncomment the following:
  pids << spawn(command)
end

pids.each do |pid| 
  Process.wait pid
end

