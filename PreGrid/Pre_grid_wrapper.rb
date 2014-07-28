#!/usr/bin/env ruby

# Ensure STDOUT buffer is flushed with each call to `puts`
$stdout.sync = true

require 'bundler/setup'
require 'yaml'
require 'securerandom'
require 'aws-sdk'
require 'json'
require 'logger'

class Pgw
# Any cmdline args passed to Pre_grid_wrapper can be accessed in the command
# below via #{ARGV[0]}, #{ARGV[1]}, etc.
def initialize(logger,credentials)
  @log = logger
  @credentials = credentials
  @pushed_files = []
  @aggregator_files = []
  @r2_aggregator_files = []
  @job_files = []
  @worker_task_queue = nil
  @worker_r2_task_queue = nil
  @job_stats = ''
  @body = {}
  @command = ''
end


def set_class_vars_from_input_file(inputfile)
  if inputfile and File.exists?(inputfile)
    input = YAML.load(File.read(inputfile))
    if input and input.is_a?(Hash)
      @job_files = Array(input['job_files'])
      # Override the pregrid command in pre_grid_command.yml with the one
      # (if any) specified in the job message.
      @command = input.fetch('pre_grid_command',
                        YAML.load(File.read('pre_grid_command.yml'))['command'])
      @worker_task_queue = input.fetch('worker_task_queue',nil)
      @worker_r2_task_queue = input.fetch('worker_r2_task_queue',nil)
      @job_id = input.fetch('job_id',SecureRandom.uuid)
      # We'll end up passing more data through the body as time goes on,
      # so we may as well just keep the full message body available.
      @body = input
    end
  else
    puts "error_messages: input file #{inputfile} does not exist."
    exit(1)
  end
  @command.gsub!('$jobid',@job_id)
  @command.gsub!('$input_file',inputfile)
end


def start
  @log.info '------------------------------------------------------------------'
  @log.info 'Pre_grid_wrapper started'
  
  # $input_file passed on cmdline contains some info about job-specific files
  # that will be used to populate part of the task header.
  set_class_vars_from_input_file(ARGV[0])
  
  task_ids = []
  r2_task_ids = []

  @w_results = @job_id + '_w_results'
  @wr2_results = @job_id + '_wr2d_results'
  create_result_queue(@w_results)
  create_result_queue(@wr2_results)

  listening = false
  r2_mode = false
  @possible_errors = []

  puts "set_queue: #{@worker_task_queue}"

  exit_status = 0
  IO.popen(@command)  do |io| 
    while !io.eof?
      msg = io.gets
      if listening
        if msg =~ /^pregrid_end_messages/
          listening = false
          @log.info 'Stopped listening for messages'
        elsif msg =~ /^push_file/
          push_file(msg)
        elsif msg =~ /^aggregator_file/
          aggregator_file(msg,r2_mode)
        elsif msg =~ /^job_statistics/
          job_statistics(msg)
        else
          task_message(msg,r2_mode,task_ids,r2_task_ids)
        end
      else
        listening,r2_mode = not_a_message(msg,r2_mode)
      end
    end
    io.close
    exit_status = $?.to_i # Propagate success or failure up the chain
  end
  
  @log.info "6k_pregrid exited with exit status #{exit_status}"

  send_aggregator_msg(task_ids,
                      @aggregator_files,
                      @body['aggregator_queue'],
                      @worker_task_queue,
                      @w_results,
                      @body['settings_for_aggregator']['aggregator_table'],
                      @body['settings_for_aggregator']['aggregator_post_process'],
                      @job_stats,
                      true)

  if (@body.fetch('aggregator_r2_queue',false)) and
     (@body['settings_for_aggregator'].fetch('aggregator_r2_table',false)) and
     (@body['settings_for_aggregator'].fetch('aggregator_r2_post_process',false))
    send_aggregator_msg(r2_task_ids,
                        @r2_aggregator_files,
                        @body['aggregator_r2_queue'],
                        @worker_r2_task_queue,
                        @wr2_results,
                        @body['settings_for_aggregator']['aggregator_r2_table'],
                        @body['settings_for_aggregator']['aggregator_r2_post_process'],
                        '',
                        false)
  end
  

  # Not strictly necessary since we exit shortly, but putting this here so
  # I remember it's required if the workflow changes later.
  @aggregator_files.clear()
  @r2_aggregator_files.clear()
  
  @log.info "Pre_grid_wrapper stopping."
  if !exit_status.zero?
    # Send possible error messages up the chain
    puts "error_messages: #{@possible_errors.join('\n').dump}"
  end
  exit exit_status
end


# Keep track of pushed files that are to be routed to workers
def push_file(msg)
  @log.info "Push file message: #{msg}"
  bucket,key = msg.sub(/^push_file\s*:\s*/,'').split(',').map{|s| s.strip}
  @pushed_files.push(Hash["bucket"=>bucket,"key"=>key])
  puts msg
  return nil
end


# Keep track of pushed files that are to be routed to aggregator
def aggregator_file(msg,r2_mode)
  @log.info "Aggregrator file message: #{msg}"
  bucket,key = msg.sub(/^aggregator_file\s*:\s*/,'').split(',').map{|s| s.strip}
  @aggregator_files.push(Hash['bucket'=>bucket,'key'=>key]) if !r2_mode
  @r2_aggregator_files.push(Hash['bucket'=>bucket,'key'=>key]) if r2_mode or key =~ /_jobdetail/
  # Map into job_breaker's push_file directive
  puts "push_file: #{bucket},#{key}"
  return nil
end


def task_message(msg,r2_mode,task_ids,r2_task_ids)
  @log.info "Task message"
  if !r2_mode
    task_ids.push( YAML.load(msg)['task_id'] )
    msg.insert(0,make_preamble(@w_results,r2_mode))
  else
    r2_task_ids.push( YAML.load(msg)['task_id'] )
    msg.insert(0,make_preamble(@wr2_results,r2_mode))
  end
  puts msg.dump
  # Don't accumulate files across tasks
  @pushed_files.clear
end


def job_statistics(msg)
  @job_stats = msg.gsub(/^job_statistics\s*:\s*/,'')
end

def not_a_message(msg,r2_mode)
  listening = false
  r2 = r2_mode
  @log.info "Non-message output: #{msg}"
  if msg =~ /^pregrid_begin_messages/
    listening = true 
    @log.info 'Listening for messages'
  elsif msg =~ /^R2D Tasks/
    puts "set_queue: #{@worker_r2_task_queue}"
    r2 = true
    @log.info 'Now in R2 task mode'
  else
    @possible_errors << msg
  end
  return [listening,r2]
end

# Form and send off aggregator queue message
#  {
#    "job_id" : "My Job ID",
#    "queue_to_aggregate" : "..."
#    "task_ids" : ["TaskID_0","TaskID_1","TaskID_...","TaskID_5999"],
#    "aggregator_files" : [{"bucket"=>b, "key"=>k},{etc}],
#    "generate_dominant_critical_soil
#  }
def send_aggregator_msg(task_ids,files,queue_name,task_queue_name,q_to_agg,db_table,post_proc,job_stats,store_inputs)
  sqs = AWS::SQS.new(@credentials)
  if !task_ids.empty?
    preamble = {}
    ezq = {}
    preamble['EZQ'] = ezq

    report_gen = { "job_id" => @job_id }

    report_gen['job_statistics'] = job_stats if !job_stats.empty?

    # The file that ends in _job.json should go in the preamble because
    # Aggregator needs that one file
    idx = files.find_index{|f| f['key'] =~ /.+_job\.json/}
    ezq['get_s3_files'] = [files[idx]] if idx
    files.delete_at(idx) if idx

    # Aggregator_r2 needs the _jobdetail.json file
    idx = files.find_index{|f| f['key'] =~ /.+_jobdetail\.json/}
    ezq['get_s3_files'] = [files[idx]] if idx
    files.delete_at(idx) if idx
    
    # The remaining files go in the body of msg to Aggregator so that it can
    # pass their refs on to ReportGen.
    report_gen['files_needed_to_make_report'] = files
    
    report_gen['queue_to_aggregate'] = q_to_agg
    report_gen['task_ids'] = task_ids
    report_gen['db_table'] = db_table
    report_gen['post_process'] = post_proc
    report_gen['store_inputs'] = store_inputs
    report_gen.merge!(@body['settings_for_aggregator'])
    msg = report_gen.to_json
    # If message is too big for SQS, divert the body into S3
    if (msg.bytesize + preamble.to_yaml.bytesize) > 256000   #256k limit minus assumed
                                                     #metadata size of 6k
                                                     #(256-6)*1024 = 256000
      msg,preamble = divert_body_to_s3(msg,preamble)
    end
    preamble = preamble.to_yaml
    preamble += "...\n"
    sqs.queues.named(queue_name).send_message(msg.insert(0,preamble))
  else
    # No tasks were generated for some reason, so delete the result queue
    sqs.queues.named(q_to_agg).delete
  end
end

# This method is copied from EZQ::Processor
# Place message body in S3 and update the preamble accordingly
def divert_body_to_s3(body,preamble)
  @log.info 'Report Gen message is too big for SQS and is beig diverted to S3'
  # Don't assume the existing preamble can be clobbered
  new_preamble = preamble.clone
  s3 = AWS::S3.new(@credentials)
  bucket_name = 'EZQOverflow.praxik'
  bucket = s3.buckets[bucket_name]
  if !bucket
    errm =  "The result message is too large for SQS and would be diverted " +
            "to S3, but the specified result overflow bucket, "+
            "#{bucket_name}, does not exist!"
    @log.fatal errm
    raise "Result overflow bucket #{bucket_name} does not exist!"
  end
  key = "overflow_body_#{@job_id}.txt"
  obj = bucket.objects.create(key,body)
  AWS.config.http_handler.pool.empty! # Hack to solve odd timeout issue
  s3_info = {'bucket'=>bucket_name,'key'=>key}
  new_preamble['EZQ'] = {} if new_preamble['EZQ'] == nil
  new_preamble['EZQ']['get_s3_file_as_body'] = s3_info
  body = "Message body was too big and was diverted to S3 as s3://#{bucket_name}/#{key}"
  return [body,new_preamble]
end


def create_result_queue(result_queue_name)
  return nil if result_queue_name == nil
  sqs = AWS::SQS.new(@credentials)
  q = sqs.queues.create(result_queue_name)
  # Block until the queue is available
  sleep (1000) until q.exists?
end


def make_preamble(result_queue_name,r2_mode)
  preamble = {}
  ezq = {}
  preamble['EZQ'] = ezq
  ezq['result_queue_name'] = result_queue_name
  @pushed_files = @pushed_files + @job_files
  ezq['get_s3_files'] = @pushed_files
  ezq['process_command'] = @body['worker_process_command'] if @body.fetch('worker_process_command',false) and !r2_mode
  ezq['process_command'] = @body['worker_2d_process_command'] if @body.fetch('worker_2d_process_command',false) and r2_mode
  preamble = preamble.to_yaml
  preamble += "...\n"
  return preamble
end

end # class


lf = File.new('Pre_grid_wrapper.log', 'a')
lf.sync = true
log = Logger.new(lf)
$stderr = lf
log.level = Logger::INFO

creds = YAML.load(File.read('credentials.yml'))
Pgw.new(log,creds).start()
