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
@man_files = []
@aggregator_files = []
@job_files = []
@gen_dom_crit_report = false
    

def start
  @log.info '------------------------------------------------------------------'
  @log.info 'Pre_grid_wrapper started'
  
  # $input_file passed on cmdline contains some info about job-specific files
  # that will be used to populate part of the task header.
  inputfile = ARGV[0]
  if inputfile and File.exists?(inputfile)
    input = YAML.load(File.read(inputfile))
    if input and input.is_a?(Hash)
      @job_files = Array(input['job_files'])
      @gen_dom_crit_report =
                     input.fetch('generate_dominant_critical_soil_report',false)
    end
  end
  
  # If a $input_file was passed down as a cmdline arg, it might already
  # contain a job_id. Need to decide if job_id will be assigned here or at the
  # web front-end.
  @job_id = SecureRandom.uuid
  @command.gsub!(/\$jobid/,@job_id)
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
          @log.info "Push file message: #{msg}"
          bucket,key = msg.sub(/^push_file\s*:\s*/,'').split(',').map{|s| s.strip}
          @pushed_files.push(Hash["bucket"=>bucket,"key"=>key])
          puts msg
        elsif msg =~ /^aggregator_file/ # Starts with 'aggregator_file'
          @log.info "Aggregrator file message: #{msg}"
          bucket,key = msg.sub(/^aggregator_file\s*:\s*/,'').split(',').map{|s| s.strip}
          @aggregator_files.push(Hash['bucket'=>bucket,'key'=>key])
          # Map into job_breaker's push_file directive
          puts "push_file: #{bucket},#{key}"
        else # This is a task to pass to job_breaker
          @log.info "Task message: #{msg}"
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

  
  #system(@command)
  #@exit_status = $?.to_i # Propagate success or failure up the chain
#
  #io = File.new('pregrid.txt')
  #listening = true
  #while !io.eof?
    #msg = io.gets
    #if listening
      #if msg =~ /^push_file/  # *Starts* with 'push_file'...
        #@log.info 'Push file message'
        #bucket,key = msg.sub(/^push_file\s*:\s*/,'').split(',').map{|s| s.strip}
        #@pushed_files.push(Hash["bucket"=>bucket,"key"=>key])
        #puts msg
      #elsif msg =~ /^aggregator_file/ # Starts with 'aggregator_file'
        #@log.info 'Aggregrator file message'
        #bucket,key = msg.sub(/^aggregator_file\s*:\s*/,'').split(',').map{|s| s.strip}
        #@aggregator_files.push(Hash['bucket'=>bucket,'key'=>key])
        ## Map into job_breaker's push_file directive
        #puts "push_file: #{bucket},#{key}"
      #else # This is a task to pass to job_breaker
        #@log.info 'Task message'
        #task_ids.push( YAML.load(msg)['task_id'] )
        #msg.insert(0,make_preamble)
        #puts msg.dump
        ## Don't accumulate files across tasks
        #@pushed_files.clear
      #end
    #else
      #@log.info "Extraneous message: #{msg}"
    #end
  #end
  #io.close
    

  
  # Form and send off Report Gen Queue message
  #  {
  #    "job_id" : "My Job ID",
  #    "task_ids" : ["TaskID_0","TaskID_1","TaskID_...","TaskID_5999"],
  #    "aggregator_files" : [{"bucket"=>b, "key"=>k},{etc}],
  #    "generate_dominant_critical_soil
  #  }
  @log.info "6k_pregrid exited with exit status #{@exit_status}"
  @log.info "Forming up message for report_gen_queue."
  sqs = AWS::SQS.new( :access_key_id => @access_key,
                      :secret_access_key => @secret_key)
  if !task_ids.empty?
    preamble = {}
    ezq = {}
    preamble['EZQ'] = ezq
    ezq['get_s3_files'] = @aggregator_files
    preamble = preamble.to_yaml
    preamble += "...\n"
    report_gen = { "job_id" => @job_id }
    report_gen['task_ids'] = task_ids
    report_gen['aggregator_files'] = @aggregator_files
    report_gen['generate_dominant_critical_soil_report'] = @gen_dom_crit_report
    msg = report_gen.to_json
    # If message is too big for SQS, divert the body into S3
    if (msg.bytesize + preamble.bytesize) > 256000   #256k limit minus assumed
                                                     #metadata size of 6k
                                                     #(256-6)*1024 = 256000
      msg,preamble = divert_body_to_s3(msg,preamble)
    end
    sqs.queues.named('6k_report_gen_test_44').send_message(msg.insert(0,preamble))
  else
    # No tasks were generated for some reason, so delete the task queue
    sqs.queues.named(@job_id).delete
  end

  # Not strictly necessary since we exit shortly, but putting this here so
  # I remember it's required if the workflow changes later.
  @aggregator_files.clear
  
  @log.info "Pre_grid_wrapper stopping with exit status #{@exit_status}"
  exit @exit_status
end


protected
  # This method is copied from EZQ::Processor
  # Place message body in S3 and update the preamble accordingly
  def divert_body_to_s3(body,preamble)
    @log.info 'Report Gen message is too big for SQS and is beig diverted to S3'
    # Don't assume the existing preamble can be clobbered
    new_preamble = preamble.clone
    s3 = AWS::S3.new
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
    s3_info = {'bucket'=>bucket_name,'key'=>key}
    new_preamble['EZQ']['get_s3_file_as_body'] = s3_info
    body = "Message body was too big and was diverted to S3 as s3://#{bucket_name}/#{key}"
    return [body,new_preamble]
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
  @pushed_files = @pushed_files + @job_files
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
