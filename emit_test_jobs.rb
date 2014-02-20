#!/usr/bin/env ruby
$stdout.sync = true

require 'json'

# Emit some push_file directives
puts "push_file: 6k_test.praxik,skel/08842505P7000_.skel"
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

# Prepare a test task
test_task = <<-END
    {
      "Task ID" : "1",
      "mmp360 input data" : { "soi_file" : "location", "skel_file" : "location", "musym" : "soilname" , "mukey" : "keyvalue", "slope" : 10.0 }
    }
    END
test_json = JSON.parse(test_task)

# The file passed in on cmdline will contain nothing but an integer. This
# simulates having a meaningful job message on which to operate. For this
# test, we will simply read that integer and enqueue that number of jobs.
max = File.read(ARGV[0]).strip.to_i

1.upto(max) do |idx|
  test_json['Task ID'] = idx.to_s
  puts test_json.to_json
  # Simulate some process requiring clock time. This is **not** necessary
  # for the functioning of this test or the enqueuing process.
  sleep(2)
end
