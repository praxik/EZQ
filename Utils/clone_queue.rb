#!/usr/bin/env ruby

require 'bundler/setup'
require 'yaml'
require 'aws-sdk'
require '../x_queue'

begin
  q = String.new(ARGV[0])
  if q.empty?
    warn 'You must specify a queue name'
    warn 'Usage: clone_queue QUEUE_NAME'
    warn ''
    warn 'This application naively assumes that a credentials.yml is in the'
    warn 'current directory'
    exit 1
  end

  credentials = YAML.load(File.read('credentials.yml'))
  AWS.config(credentials)

  queue = AWS::SQS::X_queue.new(AWS::SQS.new.queues.named(q).url)

  if !Dir.exists?(q)
    Dir.mkdir(q)
    File.write("#{q}/.gitignore",'*') # Tell git to ignore everything in this
                                      # dir. That's what we usually want with
                                      # message clones.
  end

  # Get each message from the queue using block form that autodeletes.
  puts "\nCloning queue messages from #{q}"
  puts "This may appear to hang for ~20 seconds at the end. Nothing is wrong." 
  puts "That's just the effect of the long-polling wait timeout on the queue."
  puts "Once you stop seeing 'Cloning...' messages, feel free to interrupt with ctrl-c.\n\n"
  queue.poll_no_delete(:idle_timeout => 2,:batch_size => 10) do |msg|
    Array(msg).each do |item|
      puts "Cloning message #{item.id}"
      File.write("#{q}/#{item.id}",item.body)
      item.visibility_timeout = 60
    end
  end
rescue Interrupt
  warn "\nclone_queue aborted."
end
