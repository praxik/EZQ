#!/usr/bin/env ruby

# Starts N instances of EZQ::Processor. N is pulled from key
# 'number_of_processes' in userdata.yml. Valid values are integer N:N>1 and 'auto'.
# A value of 'auto' will start as many processes as there are available cores
# (according to the OS)

require 'bundler/setup'
require 'yaml'
require 'parallel'

command = 'ruby processor.rb -c receive_queue_config.yml'
Dir.chdir(File.dirname(__FILE__))

userdata = ''
begin
  userdata = YAML.load_file('userdata.yml')
rescue
  warn 'Error opening or parsing `userdata.yml`. Aborting'
  exit(1)
end

num = userdata.fetch('number_of_processes',1)
num = num == 'auto' ?
      Parallel.processor_count :
      num.to_i

rec_queue = userdata.fetch('receive_queue_name','')
puts "Overriding receive queue setting with #{rec_queue}" if !rec_queue.empty?
err_queue = userdata.fetch('error_queue_name','')
loggly_token = userdata.fetch('loggly_token',nil)
loggly_level = userdata.fetch('loggly_level',nil)
app_name = userdata.fetch('app_name',nil)
pry_server = userdata.fetch('pry_server',false)
debug_port = 7755
exit_port = 8642
pids = []

num.times do |idx|
  debug_port += idx
  exit_port += idx
  command += " --log ./processor_" + "%02d" % idx + ".log"
  command += " --log_severity info"
  command += " --queue #{rec_queue}" if !rec_queue.empty?
  command += " --error_queue #{err_queue}" if !err_queue.empty?
  command += " --token #{loggly_token}" if loggly_token
  command += " --loggly_severity #{loggly_level}" if loggly_level
  command += " --app_name #{app_name}" if app_name
  command += " --debug_port #{debug_port}" if pry_server
  command += " --exit_port #{exit_port}"
  pids << spawn(command)
end

pids.each{|pid| Process.detach(pid)}
