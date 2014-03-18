#!/usr/bin/env ruby

# Ensure STDOUT buffer is flushed with each call to `puts`
$stdout.sync = true

require 'bundler/setup'
require 'yaml'
require 'securerandom'
require 'aws-sdk'

# Any cmdline args passed to Pre_grid_wrapper can be accessed in the command
# below via #{ARGV[0]}, #{ARGV[1]}, etc.
@command = "./emit_test_jobs.rb #{ARGV[0]}"
@pushed_files = []
@access_key = ''
@secret_key = ''


def start
  # If a $input_file was passed down as a cmdline arg, it might already
  # contain a job_id. Need to decide if job_id will be assigned here or at the
  # web front-end.
  @job_id = SecureRandom.uuid
  task_ids = []
  create_result_queue
  listening = false
  IO.popen(@command)  do |io| 
    while !io.eof?
      msg = io.gets
      if listening
        if msg =~ /^pregrid_end_messages/ # Stop listening when we get this.
          listening = false
        elsif msg =~ /^push_file/  # *Starts* with 'push_file'...
          bucket,key = msg.sub(/^push_file\s*:\s*/,'').split(',').map{|s| s.strip}
          @pushed_files.push(Hash["bucket"=>bucket,"key"=>key])
          puts msg
        else
          task_ids.push( YAML.load(msg)['task_id'] )
          msg.insert(0,make_preamble)
          puts msg.dump
          # Don't accumulate files across tasks
          @pushed_files.clear
        end
      else
        if msg =~ /^pregrid_begin_messages/ # @command will only output
          listening = true                  # valid messages now. It promises.
      end
    end
  end
  # Form and send off Report Gen Queue message
  #  {
  #    "Job ID" : "My Job ID",
  #    "Task IDs" : ["TaskID_0","TaskID_1","TaskID_...","TaskID_5999"]
  #  }
  report_gen = { "Job ID" => @job_id }
  report_gen['Task IDs'] = task_ids
  sqs = AWS::SQS.new( :access_key_id => @access_key,
                      :secret_access_key => @secret_key)
  sqs.queues['6k_report_gen_test_44'].send_message(report_gen.to_yaml)
end

def create_result_queue
  sqs = AWS::SQS.new( :access_key_id => @access_key,
                      :secret_access_key => @secret_key)
  sqs.queues.create("#{@job_id}")
end

def make_preamble
  preamble = {}
  ezq = {}
  preamble['EZQ'] = ezq
  ezq['result_queue_name'] = @job_id
  ezq['get_s3_files'] = @pushed_files unless @pushed_files.empty?
  preamble = preamble.to_yaml
  preamble += "...\n"
  return preamble
end

creds = YAML.load(File.read('credentials.yml'))
@access_key = creds['access_key_id']
@secret_key = creds['secret_access_key']
start
