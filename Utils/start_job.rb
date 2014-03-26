#!/usr/bin/env ruby

require 'bundler/setup'
require 'yaml'
require 'aws-sdk'

credentials = YAML.load(File.read('../credentials.yml'))
AWS.config(credentials)
job_queue = AWS::SQS.new.queues.named('6k_job_test_44')
job_queue.send_message('Start')

puts 'Sent message to start job.'
