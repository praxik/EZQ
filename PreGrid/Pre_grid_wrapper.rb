#!/usr/bin/env ruby

# Ensure STDOUT buffer is flushed with each call to `puts`
$stdout.sync = true

require 'bundler/setup'
require 'yaml'
require 'securerandom'
require 'aws-sdk'
require 'json'
require 'logger'

# Any cmdline args passed to Pre_grid_wrapper can be accessed in the command
# below via #{ARGV[0]}, #{ARGV[1]}, etc.
@command = YAML.load(File.read('pre_grid_command.yml'))['command']
@pushed_files = []
@access_key = ''
@secret_key = ''
@man_bucket = ''
@man_key = ''
    

def start
  @log.info '------------------------------------------------------------------'
  @log.info 'Pre_grid_wrapper started'
  # $input_file passed on cmdline contains some info about management files that
  # will be used to populate part of the task header.
  inputfile = ARGV[0]
  if File.exists?(inputfile)
    input = YAML.load(File.read(inputfile))
    @man_bucket = input['man_bucket']
    @man_key = input['man_key']
  end
  
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
          @log.info 'Stopped listening for messages'
        elsif msg =~ /^push_file/  # *Starts* with 'push_file'...
          @log.info 'Push file message'
          bucket,key = msg.sub(/^push_file\s*:\s*/,'').split(',').map{|s| s.strip}
          @pushed_files.push(Hash["bucket"=>bucket,"key"=>key])
          puts msg
        else # This is a task to pass to job_breaker
          @log.info 'Task message'
          task_ids.push( YAML.load(msg)['task_id'] )
          msg.insert(0,make_preamble)
          puts msg.dump
          # Don't accumulate files across tasks
          @pushed_files.clear
        end
      else
        @log.info "Extraneous message: #{msg}"
        if msg =~ /^pregrid_begin_messages/
          listening = true 
          @log.info 'Listening for messages'
          # @command will only output valid messages now. It promises.
        end
      end
    end
    io.close
    @exit_status = $?.to_i # Propagate success or failure up the chain
  end
  
  # Form and send off Report Gen Queue message
  #  {
  #    "Job ID" : "My Job ID",
  #    "Task IDs" : ["TaskID_0","TaskID_1","TaskID_...","TaskID_5999"]
  #  }
  sqs = AWS::SQS.new( :access_key_id => @access_key,
                      :secret_access_key => @secret_key)
  if !task_ids.empty?
    report_gen = { "job_id" => @job_id }
    report_gen['task_ids'] = task_ids
    sqs.queues.named('6k_report_gen_test_44').send_message(report_gen.to_json)
  else
    # No tasks were generate for some reason, so delete the task queue
    sqs.queues.named(@job_id).delete
  end
  
  @log.info "Pre_grid_wrapper stopping with exit status #{@exit_status}"
  exit @exit_status
end

def create_result_queue
  sqs = AWS::SQS.new( :access_key_id => @access_key,
                      :secret_access_key => @secret_key)
  q = sqs.queues.create("#{@job_id}")
  # Block until the queue is available
  sleep (1000) until q.exists?
end

def make_preamble
  preamble = {}
  ezq = {}
  preamble['EZQ'] = ezq
  ezq['result_queue_name'] = @job_id
  # Add management file/archive to list of files to be fetched.
  decompress = File.extname(@man_key) == '.zip' ? true : false
  @pushed_files.push(Hash["bucket"=>@man_bucket,
                          "key"=>@man_key,
                          "decompress"=>decompress]) unless @man_key.empty?
  ezq['get_s3_files'] = @pushed_files
  preamble = preamble.to_yaml
  preamble += "...\n"
  return preamble
end

lf = File.new('Pre_grid_wrapper.log', 'a')
lf.sync = true
@log = Logger.new(lf)
$stderr = lf
@log.level = Logger::INFO

creds = YAML.load(File.read('credentials.yml'))
@access_key = creds['access_key_id']
@secret_key = creds['secret_access_key']
start
