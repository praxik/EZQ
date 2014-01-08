#!/usr/bin/env ruby

require './deps/configulator'

begin
  cf = Configulator.new

  queue = AWS::SQS.new.queues.named(cf.queue_name)

  # Get each message from the queue using block form that autodeletes.
  puts "\nClearing queue #{cf.queue_name}."
  puts "This may appear to hang for ~20 seconds at the end. Nothing is wrong." 
  puts "That's just the effect of the long-polling wait timeout you've set on the queue."
  puts "Once you stop seeing 'Deleting...' messages, feel free to interrupt with ctrl-c.\n\n"
  queue.poll(:idle_timeout => 2) do |msg|
    puts "Deleting message #{msg.id}"
  end
rescue Interrupt
  warn "\nclear_queue aborted."
end
