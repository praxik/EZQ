#!/usr/bin/env ruby

# Slightly modified version of the oad-balancing broker from the
# ZMQ  Ruby examples

require 'rubygems'
require 'ffi-rzmq'


def start
  @context = ZMQ::Context.new()
  @frontend = @context.socket(ZMQ::ROUTER)
  @backend = @context.socket(ZMQ::ROUTER)

  @frontend.bind('tcp://*:4443')
  @backend.bind('tcp://*:4445')

  available_workers = []
  poller = ZMQ::Poller.new()
  poller.register_readable(@backend)
  poller.register_readable(@frontend)

  # The poller will continuously poll the backend and will poll the
  # frontend when there is at least one worker available.
  loop do
  poller.poll
    poller.readables.each do |readable|
      if readable === @backend
        @backend.recv_string worker = ""
        @backend.recv_string empty = ""
        @backend.recv_strings reply = []

        @frontend.send_strings reply unless reply[0] == "READY"

        # Add this worker to the list of available workers
        available_workers << worker
      elsif readable === @frontend && available_workers.any?
        # Read the request from the client and forward it to the LRU worker
        @frontend.recv_strings request = []
        @backend.send_strings [available_workers.shift, ""] + request
      end
    end
  end
  puts "error"

  @frontend.close
  @backend.close
  @context.terminate
end


begin
  start()
rescue Interrupt
  puts "Load balancer killed by user. Cleaning up."
  @frontend.close
  @backend.close
  @context.terminate
end
