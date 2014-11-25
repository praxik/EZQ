#!/usr/bin/env ruby

port = ARGV.shift.to_i

if !port
  puts "Usage: term_processor PORT"
  exit(1)
end

s = TCPSocket.new('localhost',port)
s.puts "TERMINATE"
s.close
