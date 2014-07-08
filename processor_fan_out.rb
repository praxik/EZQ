#!/usr/bin/env ruby

# This script runs EZQ::Processor in each of the 44 separate deploy subdirs.

require 'bundler/setup'
require 'yaml'
#require 'aws-sdk'
#require 'net/http'
#require 'uri'

command = 'ruby processor.rb -c mmp_example_config.yml'
Dir.chdir(File.dirname(__FILE__))

@num = 1

begin
  puts 'Looking for number of processors to run in userdata...'
#  credentials = YAML.load(File.read('credentials.yml'))
#  AWS.config(credentials)
#  uri = URI.parse("http://169.254.169.254/latest/meta-data/instance-id")
#  instance_id = Net::HTTP.get_response(uri).body
#  instance = AWS::EC2.new.instances[instance_id]
#  raise unless instance.exists?#Using this pattern because either of the 
  # two previous calls can raise an exception, and I want to do exactly the
  # same thing in any of these cases.
#  userdata = YAML.load(instance.user_data)
  userdata = YAML.load(File.read('userdata.yml'))
  @num = userdata['number_of_processes'].to_i
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
  #Dir.chdir("%02d" % idx) {pids << spawn(command)}
  # To run directly in place rather than in numbered subdirs, comment
  # out the previous line and uncomment the following:
  pids << spawn(command)
end

pids.each do |pid| 
  Process.wait pid
end

