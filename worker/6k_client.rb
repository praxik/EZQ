#!/usr/bin/env ruby

require 'optparse'
require 'ffi-rzmq'

# Previous process commnad was
#"6k_worker.exe --gdbname iowammp.gdb --inputfilename $input_file --workerid $pid -o output_$id.txt"

# This now becomes
# 6k_client --inputfilename $input_file -o output_$id.txt -t 360
# and the gdb and workerid are set on each 6k_worker by run.rb when N number
# of 6k_worker.exe are started up.

def start
  input = ''
  output = ''
  timeout = 300 # 5 minutes as seconds

  op = OptionParser.new do |opts|
    opts.banner = "Usage: 6k_client.rb [options]"

    opts.on("-i", "--inputfilename [FILE]", "Name of JSON inputfile") do |file|
      input = file
    end
    opts.on("-o", "--outputfilename [FILE]","Name of file to which to write results") do |file|
      output = file
    end
    opts.on("-t", "--timeout [SECONDS]","Timeout after so many SECONDS") do |seconds|
      timeout = seconds.to_i
    end
  end

  begin op.parse! ARGV
  rescue OptionParser::InvalidOption => e
    exit(1)
  end

  begin
    process_task(input,output,timeout)
  # Handle Ctrl-C gracefully
  rescue Interrupt
    exit(1)
  end
end


# Process the worker task
# @param input_file Name of input file to 6k_worker
# @param output_file Name of file 6k_worker should store results in
# @param timeout Timeout in seconds to wait for response from worker before giving up
def process_task(input_file,output_file,timeout)
  # Try only once, and timeout after specified number of seconds
  server = LPClient.new("tcp://localhost:4443", 1, timeout*1000)
  server.send("#{input_file},#{output_file}") do |reply|
    if reply == 'success'
      puts "worker replied with success"
      exit(0)
    else
      puts reply
      exit(1)
    end
  end
end

# Lazy pirate class from Han Holl <han.holl@pobox.com>
# Copied from ZMQ examples.
# timeout is in milliseconds
class LPClient
  def initialize(connect, retries = nil, timeout = nil)
    @connect = connect
    @retries = (retries || 1).to_i
    @timeout = (timeout || 10).to_i
    @ctx = ZMQ::Context.new(1)
    client_sock()
    set_poller()
    at_exit do
      @socket.close()
    end
  end
  
  def client_sock
    @socket = @ctx.socket(ZMQ::REQ)
    @socket.setsockopt(ZMQ::LINGER, 0)
    @socket.connect(@connect)
  end

  def set_poller
    @poller = ZMQ::Poller.new()
    @poller.register_readable(@socket)
  end

  def send(message)
    @retries.times do |tries|
      if !@socket.send_string(message)
        yield "Socket error sending message"
        return
      end      
      @poller.poll(@timeout)
      if !@poller.readables.empty?
        @socket.recv_string(msg = '')
        yield msg
        return
      else
        @socket.close
        client_sock
      end
    end
    yield "Timed out waiting for backend"
  end
      
end



start()
