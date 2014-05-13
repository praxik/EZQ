#!/usr/bin/env ruby

require 'bundler/setup'
require 'yaml'
require 'aws-sdk'

begin
  q = String.new(ARGV[0])
  if q.empty?
    warn 'You must specify a queue name'
    warn 'Usage: stash_queue QUEUE_NAME'
    warn ''
    warn 'This application naively assumes that a credentials.yml is in the'
    warn 'current directory'
    exit 1
  end

  credentials = YAML.load(File.read('credentials.yml'))
  AWS.config(credentials)

  queue = AWS::SQS.new.queues.named(q)

  if !Dir.exists?(q)
    Dir.mkdir(q)
    File.write("#{q}/.gitignore",'*') # Tell git to ignore everything in this
                                      # dir. That's what we usually want with
                                      # message stashes.
  end

  # Get each message from the queue using block form that autodeletes.
  puts "\nStashing queue messages#{q}"
  puts "This may appear to hang for ~20 seconds at the end. Nothing is wrong." 
  puts "That's just the effect of the long-polling wait timeout on the queue."
  puts "Once you stop seeing 'Stashing...' messages, feel free to interrupt with ctrl-c.\n\n"
  queue.poll(:idle_timeout => 2,:batch_size => 10) do |msg|
    Array(msg).each do |item|
      puts "Stashing message #{item.id}"
      File.write("#{q}/#{item.id}",item.body)
    end
  end
rescue Interrupt
  warn "\nstash_queue aborted."
end
