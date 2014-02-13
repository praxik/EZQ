#!/usr/bin/env ruby
$stdout.sync = true

require 'json'

# Emit some push_file directives
puts "push_file: bucket1,key1"
puts "push_file: bucket2,key2"

# Emit a batch of tasks
file = File.read('test_task.json')
json = JSON.parse(file)
json['tasks'].each do |job| 
  puts job.to_json
  # Simulate some step that requires clock time
  sleep(1)
end
