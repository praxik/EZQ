#!/usr/bin/env ruby

require 'rubygems'
require 'ffi-rzmq'

def start
  @context = ZMQ::Context.new
  @socket = @context.socket(ZMQ::REQ)
  @socket.connect('tcp://localhost:4445')

  # Lengthy initialization here.
  sleep(2)
  puts "end init"

  # Let load balancer know this worker is ready to consume tasks.
  @socket.send_string("READY")

  loop do
    @socket.recv_string client = ""
    @socket.recv_string empty = ""
    @socket.recv_string(task = '')
    puts "Received request: #{task}"

    # Process the task
    sleep(2)

    # Send success if we didn't crash while processing the task
    @socket.send_strings([client, empty, "success"])
  end
end

begin
  start()
rescue Interrupt
  @socket.close() if @socket
  @context.terminate() if @context
end
