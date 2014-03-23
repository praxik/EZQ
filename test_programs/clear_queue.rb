#!/usr/bin/env ruby

require './deps/configulator'

begin
  cf = Configulator.new

  q = String.new(ARGV[0])
  q = q.empty? ? cf.queue_name : q

  queue = AWS::SQS.new.queues.named(q) 

  # Get each message from the queue using block form that autodeletes.
  puts "\nClearing queue #{cf.queue_name}."
  puts "This may appear to hang for ~20 seconds at the end. Nothing is wrong." 
  puts "That's just the effect of the long-polling wait timeout you've set on the queue."
  puts "Once you stop seeing 'Deleting...' messages, feel free to interrupt with ctrl-c.\n\n"
  queue.poll(:idle_timeout => 2,:batch_size => 10) do |msg|
    Array(msg).each {|item| puts "Deleting message #{item.id}"}
  end
rescue Interrupt
  warn "\nclear_queue aborted."
end
