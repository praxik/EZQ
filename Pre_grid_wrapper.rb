#!/usr/bin/env ruby

# Ensure STDOUT buffer is flushed with each call to `puts`
$stdout.sync = true

require 'yaml'
require 'securerandom'
require 'aws-sdk'

@command = './emit_test_jobs.rb'
@pushed_files = []
@access_key = ''
@secret_key = ''


def start
  @job_id = SecureRandom.uuid
  create_result_queue
  IO.popen([@command])  do |io| 
    while !io.eof?
      msg = io.gets
      if msg =~ /^push_file/
        bucket,key = msg.sub(/^push_file\s*:\s*/,'').split(',').map{|s| s.strip}
        @pushed_files.push(Hash["bucket"=>bucket,"key"=>key])
        puts msg
      else
        msg.insert(0,make_preamble)
        puts msg.dump
      end
    end
  end
end

def create_result_queue
  #puts "Would be creating result queue named '#{@job_id}'"
  sqs = AWS::SQS.new( :access_key_id => @access_key,
                      :secret_access_key => @secret_key)
  sqs.queues.create("#{@job_id}")
end

def make_preamble
  @preamble ||= begin
    @preamble = {}
    ezq = {}
    @preamble['EZQ'] = ezq
    ezq['result_queue_name'] = @job_id
    ezq['get_s3_files'] = @pushed_files unless @pushed_files.empty?
    @preamble = @preamble.to_yaml
    @preamble += "...\n"
    #puts @preamble.inspect
  end
end

config = YAML.load(File.read('pre_grid_config.yml'))
@access_key = config['access_key_id']
@secret_key = config['secret_access_key']
start
