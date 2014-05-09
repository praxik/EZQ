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
    

def start
  @log.info '------------------------------------------------------------------'
  @log.info 'Pre_grid_wrapper started'
  
  # $input_file passed on cmdline contains some info about job-specific files
  # that will be used to populate part of the task header.
  inputfile = ARGV[0]
  if File.exists?(inputfile)
    input = YAML.load(File.read(inputfile))
    @job_files = Array(input['job_files'])
    @gen_dom_crit_report =
                     input.fetch('generate_dominant_critical_soil_report',false)
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
          @log.info 'Push file message'
          bucket,key = msg.sub(/^push_file\s*:\s*/,'').split(',').map{|s| s.strip}
          @pushed_files.push(Hash["bucket"=>bucket,"key"=>key])
          puts msg
        elsif msg =~ /^aggregator_file/ # Starts with 'aggregator_file'
          @log.info 'Aggregrator file message'
          bucket,key = msg.sub(/^aggregator_file\s*:\s*/,'').split(',').map{|s| s.strip}
          @aggregator_files.push(Hash['bucket'=>bucket,'key'=>key])
          # Map into job_breaker's push_file directive
          puts "push_file: #{bucket},#{key}"
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
  #    "job_id" : "My Job ID",
  #    "task_ids" : ["TaskID_0","TaskID_1","TaskID_...","TaskID_5999"],
  #    "aggregator_files" : [{"bucket"=>b, "key"=>k},{etc}],
  #    "generate_dominant_critical_soil
  #  }
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
    sqs.queues.named('6k_report_gen_test_44').send_message(report_gen.to_json.insert(0,preamble))
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
