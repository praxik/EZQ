#!/usr/bin/env ruby

require 'bundler/setup'
require 'yaml'
require 'aws-sdk'

# Queue name is the path leaf
q_dir = File.basename(ARGV[0])

credentials = YAML.load(File.read('credentials.yml'))
AWS.config(credentials)

queue = AWS::SQS.new.queues.named(q_dir)

files = Dir.entries(q_dir)
# Skip any files that begin with '.'
files.delete_if {|f| f =~ /\.+/}
files.each{|f| queue.send_message(File.read("#{q_dir}/#{f}"))}
