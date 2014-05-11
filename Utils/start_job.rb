#!/usr/bin/env ruby

require 'bundler/setup'
require 'yaml'
require 'aws-sdk'

credentials = YAML.load(File.read('credentials.yml'))
AWS.config(credentials)
job_queue = AWS::SQS.new.queues.named('6k_job_test_44')

job_hash = {}

job_config = ARGV[0] != nil ? ARGV[0] : 'job_config.yml'

msg = File.read(job_config)

#job_hash = YAML.load(File.read(job_config))

#job_queue.send_message(job_hash.to_yaml)
job_queue.send_message(msg)

puts 'Sent following message to job queue:'
#puts job_hash.to_yaml
puts msg
