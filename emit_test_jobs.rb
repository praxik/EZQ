#!/usr/bin/env ruby
$stdout.sync = true

require 'json'

# Emit some push_file directives
puts "push_file: 6k_test,skel/08842505P7000_.skel"

puts "push_file: 6k_test.praxik,soils/IA015_2550232-543033.soi"
puts "push_file: 6k_test.praxik,soils/IA015_2550235-543040.soi"
puts "push_file: 6k_test.praxik,soils/IA015_2550236-543041.soi"
puts "push_file: 6k_test.praxik,soils/IA015_2550237-543042.soi"
puts "push_file: 6k_test.praxik,soils/IA015_2550271-543103.soi"
puts "push_file: 6k_test.praxik,soils/IA015_2550276-543110.soi"
puts "push_file: 6k_test.praxik,soils/IA015_2550279-543018.soi"
puts "push_file: 6k_test.praxik,soils/IA015_2550285-1235792.soi"
puts "push_file: 6k_test.praxik,soils/IA015_2550309-543031.soi"
puts "push_file: 6k_test.praxik,soils/IA153_412834-560959.soi"
puts "push_file: 6k_test.praxik,soils/IA153_412837-560961.soi"
puts "push_file: 6k_test.praxik,soils/IA153_412839-560963.soi"
puts "push_file: 6k_test.praxik,soils/IA153_412963-560923.soi"
puts "push_file: 6k_test.praxik,soils/IA153_412965-560925.soi"

# Emit a batch of tasks
file = File.read('test_worker_task_data.json')
json = JSON.parse(file)
json['tasks'].each do |job| 
  puts job.to_json
  # Simulate some step that requires clock time
  sleep(1)
end
