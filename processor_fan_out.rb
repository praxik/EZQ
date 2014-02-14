#!/usr/bin/env ruby

# This script runs EZQ::Processor in each of the 44 separate deploy subdirs.

#command ="echo $PWD"
command = 'ruby processor.rb'
Dir.chdir(File.dirname(__FILE__))

pids = []

44.times do |idx|
  Dir.chdir("%02d" % idx) {pids << spawn(command)}
end

pids.each do |pid| 
  Process.wait pid
end

