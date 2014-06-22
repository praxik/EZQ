#!/usr/bin/env ruby

require 'bundler/setup'
require 'json'
require 'yaml'
require 'aws-sdk'
require 'logger'
require 'digest/md5'
require 'deep_merge'
require 'securerandom'

module EZQ

# FilePusher class pushes a file specified in the form bucket,key out to S3
# using the supplied credentials and logging to the supplied logger. It is
# intended to be used as a thread object.
class FilePusher
  def initialize(bucket_comma_filename,dry_run,credentials,logger)
    bname,fname = bucket_comma_filename.split(',').map{|s| s.strip}
    if dry_run
      puts "Would be pushing '#{fname}' into bucket '#{bname}'"
      return
    end
    logger.info "Pushing #{bucket_comma_filename}"
    if File.exists?(fname)
      s3 = AWS::S3.new(credentials)
      bucket = s3.buckets[bname]
      obj = bucket.objects.create(fname,Pathname.new(fname))
      AWS.config.http_handler.pool.empty!
    else
      logger.error "file #{fname} does not exist; can't push it to S3."
      return nil
    end
    logger.info "Successfully pushed file #{fname} to S3."
    return nil
  end
end

# Job_Breaker takes a JSON structure containing separate tasks, and enqueues
# each of the separate tasks as specified in the configuration. A *Job* is
# a collection of *Tasks*.
#
# There are two ways to use Job_Breaker: Give it a single string containing 
# all the tasks in a job, like this:
# 
#     {
#        "tasks": 
#        [
#          {
#            "Task ID" : "Task 1",
#            ...
#          },
#          {
#            "Task ID" : "Task 2",
#            ...
#          },
#          ... etc ...
#        ]
#     }
#
# Or, set a job_creator_command in the configuration. Job_Breaker will run this
# command, and listen on STDOUT for individual tasks. Each task should be
# formatted as JSON, and should be emitted as a *single* *string*.
# Ruby programs acting as job_creators should set <tt>$stdout.sync = true</tt> 
# to ensure the STDOUT buffer is flushed each time +puts+ is called.
#
# A single task looks like this:
#
#    {
#      "Task ID" : "Task 1",
#      ...
#    }
#
# When creating a batch of Tasks from a Job, it is often desirable to cache
# files needed for the Tasks in Amazon S3. Job_Breaker provides a simple 
# mechanism to support this need without having to bake it directly into the 
# job_creator_command. To use this, have the job_creator_command write the 
# file to local storage, and then put a message formatted like this on STDOUT:
#
#     push_file: bucket,filename
#
# where bucket refers to the S3 bucket in which to place the file, and filename
# is the name of the local file. Filename will also be used as the target name
# in the S3 bucket. Job_Breaker detects these messages beginning with push_file
# and recognizes them as a command rather than as a Task to enqueue.
class Job_Breaker

  public
  # Creates and starts a Job_Breaker instance
  #
  # @param [Hash] config A configuration hash
  # @param [Hash] credentials The AWS credentials hash to use. It should contain
  #   two key-value pairs: access_key_id and secret_access_key_id. See
  #   http://aws.amazon.com/security-credentials for more information about the
  #   value of each key.
  # @param [String] job_string A JSON job string that will be broken up into
  #                 separate tasks and enqueued. This value will be ignored if
  #                 config::job_creator_command is set.
  # @param [Logger] logger The logger to use for logging internal output 
  def initialize(config,credentials,job_string = '',logger = nil)
    if !logger
      logger = Logger.new(STDOUT)
      logger.level = Logger::INFO
    end
    @logger = logger
    
    # Set up AWS with the specified credentials
    #~credentials = {}
    #~credentials['access_key_id'] = config['access_key_id']
    #~credentials['secret_access_key'] = config['secret_access_key']
    @credentials = credentials
    AWS.config(credentials)
    
    @job_creator_command = config['job_creator_command']
    @task_queue_name = config['task_queue_name']
    @preamble = config['preamble']
    @repeat_message_n_times = config['repeat_message_n_times']
    @repeat_message_type = config['repeat_message_type']
    @dry_run = config['dry_run']
    @enqueued_tasks = []
    @already_pushed = []
   
    get_queue unless @dry_run
    if job_string.empty? && !@job_creator_command.empty?
      wrap_job_creator
    else  
      split_job(JSON.parse(job_string))
    end
    @logger.info "Exit status #{@exit_status}"
    exit @exit_status
  end
  
  
  
  protected
  # Call into AWS to get the result queue and put it into the variable 
  # @result_queue. No call to AWS is made if the result queue already exists
  # and has the name of the currently-specified queue.
  def get_queue
	@logger.info "Getting queue '#{@task_queue_name}'"
	begin
		@queue = AWS::SQS.new.queues.named(@task_queue_name)
	rescue
	  @logger.fatal "No queue named #{@task_queue_name} exists."
      raise "No queue named #{@task_queue_name} exists."
	end
    if !@queue.exists?
      @logger.fatal "No queue named #{@task_queue_name} exists."
      raise "No queue named #{@task_queue_name} exists."
    end
  end
  
  
  protected
  # Starts up the job_creator_command and listens to its STDOUT for tasks
  def wrap_job_creator
    @logger.info "Running: #{@job_creator_command}"
    push_threads = []
    IO.popen(@job_creator_command)  do |io| 
      while !io.eof?
        msg = io.gets
        msg = unescape(msg)
        msg.sub!(/^"/,'') # Remove initial wrapping quote
        msg.sub!(/"$/,'') # Remove final wrapping quote
        if msg =~ /^push_file/
          # Don't push the same file multiple times during a job.
          bucket_comma_filename = msg.sub!(/^push_file\s*:\s*/,'')
          if !@already_pushed.include?(bucket_comma_filename)
            push_threads << Thread.new(bucket_comma_filename, @dry_run, @credentials, @logger){ |b,d,c,l| FilePusher.new(b,d,c,l) }
            @already_pushed << bucket_comma_filename
          end
        elsif msg =~ /^error_messages: /
          puts msg # Propagate error messages up to parent processor
        elsif msg =~ /^set_queue/
          # FIXME: this business of changing queues will break non-inline
          # message repetition.
          @task_queue_name = msg.sub!(/^set_queue\s*:\s*/,'').strip
          get_queue()
        else
          body,preamble = make_preamble(msg)
          msg = enqueue_task(body,preamble)
          @enqueued_tasks.push(msg)
        end
      end
      io.close
      @exit_status =  $?.to_i
    end
    # Wait for the file pushes to finish
    push_threads.each { |t| t.join }
    if @repeat_message_type == 'collection'
      @repeat_message_n_times.times do
        @enqueued_tasks.each{|task| enqueue_task(task)}
      end
    end
  end
  
  
  
  protected
  # Breaks up a job batch into individual tasks and enqueues then
  def split_job(json)
    json['tasks'].each do |task|
      body,preamble = make_preamble(task.to_json)
      msg = enqueue_task(body,preamble)
      @enqueued_tasks.push(msg)
    end
  end


  protected
  # Enqueues a task, handling inline repeats if specified
  def enqueue_task(body,preamble)
    enqueue_task_impl(body,preamble)
    if @repeat_message_type == 'inline'
      @repeat_message_n_times.times {enqueue_task_impl(body,preamble)}
    end
  end
  
  
  protected
  # Does the true heavy-lifting of enqueueing a task
  def enqueue_task_impl(body,preamble)
    if (body.bytesize + preamble.to_yaml.bytesize) > 256000  #256k limit minus assumed
                                                     #metadata size of 6k
                                                     #(256-6)*1024 = 256000
      body,preamble = divert_body_to_s3(body,preamble)
    end
    msg = "#{preamble.to_yaml}...\n#{body}"
    if @dry_run
      puts "#Would be enqueuing this: #{msg}"
      return
    end
    digest = Digest::MD5.hexdigest(msg)
    sent = @queue.send_message(msg)
    if digest == sent.md5
      @logger.info "Enqueued msg #{sent.id} in queue '#{@task_queue_name}'"
    else
      @logger.error "Failed to enqueue msg:\n#{msg}\n-----------------------"
    end
  end


  protected
  # Deep merges the config preamble with the task-embedded preamble, favoring
  # the task-embedded preamble in any conflicts.
  def make_preamble(task)
    preamble = @preamble
    task_pa = YAML.load(task)
    if task_pa.kind_of?(Hash) && task_pa.has_key?('EZQ')
      pa = YAML.load(preamble)
      task_pa.deep_merge(pa)
      #preamble = "#{task_pa.to_yaml}\n..."
      preamble = task_pa
    end
    return [task.sub(/-{3}\nEZQ.+?\.{3}\n/m,''),preamble]
    #return "#{preamble}\n#{task.sub(/-{3}\nEZQ.+?\.{3}\n/m,'')}"
  end

  # This method is copied from EZQ::Processor
  # Place message body in S3 and update the preamble accordingly
  def divert_body_to_s3(body,preamble)
    @logger.info 'Message is too big for SQS and is beig diverted to S3'
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
    key = "overflow_body_#{SecureRandom.uuid}.txt"
    obj = bucket.objects.create(key,body)
    AWS.config.http_handler.pool.empty! # Hack to solve odd timeout issue
    s3_info = {'bucket'=>bucket_name,'key'=>key}
    new_preamble['EZQ']['get_s3_file_as_body'] = s3_info
    body = "Message body was too big and was diverted to S3 as s3://#{bucket_name}/#{key}"
    return [body,new_preamble]
  end


  protected
  # Un-escapes an escaped string. Cribbed from
  # http://stackoverflow.com/questions/8639642/whats-the-best-way-to-escape-and-unescape-strings
  # 
  def unescape(str)
    # Escape all the things
    str.gsub(/\\(?:([#{UNESCAPES.keys.join}])|u([\da-fA-F]{4}))|0?x([\da-fA-F]{2})/) {
      if $1
        UNESCAPES[$1] # escape characters
      elsif $2 # escape \u0000 unicode
        ["#$2".hex].pack('U*')
      elsif $3 # escape \0xff or \xff
        [$3].pack('H2')
      end
    }
  end

  UNESCAPES = { 'a' => "\x07", 'b' => "\x08", 't' => "\x09",
                'n' => "\x0a", 'v' => "\x0b", 'f' => "\x0c",
                'r' => "\x0d", 'e' => "\x1b", '\\' => '\\',
                "\"" => "\x22", "'" => "\x27" }
  
end #class
end #module

################################################################################
# Run this bit if this file is being run directly as an executable rather than 
# being imported as a module.
if __FILE__ == $0
  require 'optparse'
  
  quiet = false
  config_file = 'job_breaker_config.yml'
  jobs_file = ''
  preamble = ''
  creds_file = 'credentials.yml'
  jcc = ''
  dry_run = false
  log_file = STDOUT
  op = OptionParser.new do |opts|
    opts.banner = "Usage: job_breaker.rb [options]"

    opts.on("-q", "--quiet", "Run quietly") do |q|
      quiet = q
    end
    opts.on("-c", "--config [CONFIG_FILE]", "Use configuration file CONFIG_FILE. The file ./job_breaker_config.yml is used if this option is not specified.") do |file|
      config_file = file
    end
    opts.on("-l", "--log [LOG_FILE]","Log to file LOG_FILE. STDOUT is used if this option is not specified.") do |file|
      log_file = file
    end
    opts.on("-j", "--jobs [JOBS_FILE]","Read jobs from file JOBS_FILE") do |file|
      jobs_file = file
    end
    opts.on("-p", "--preamble [PREAMBLE]","Overrides the preamble set in the configuration file with contents of the string PREAMBLE") do |text|
      preamble = text
    end
    opts.on("-r", "--credentials [CREDS_FILE]","Use credentials file CREDS_FILE. The file ./credentials.yml is used if this option is not specified.") do |file|
      creds_file = file
    end
    opts.on("-e", "--execute [COMMAND_STRING]","Override the job_creator_command specified in the configuration file with COMMAND_STRING") do |cmd|
      jcc = cmd
    end
    opts.on("-d", "--dry-run","Output tasks to STDOUT rather than placing into real queue. This is useful for checking the output of a job_creator_command while setting up a workflow.") do |d|
      dry_run = true
    end
  end

  begin op.parse! ARGV
  rescue OptionParser::InvalidOption => e
    if !quiet
      puts e
      puts op
    end
    exit 1
  end
  
  
  job = jobs_file.empty? ? '' : File.read(jobs_file)
  
  begin
    puts "EZQ.Job_Breaker started.\n\n" unless quiet
    log = Logger.new(log_file)
    if quiet && log_file == STDOUT
      log.level = Logger::UNKNOWN
    else
      log.level = Logger::INFO
    end
    
    log.info "Parsing configuration file #{config_file}"
    config_file = File.join(File.dirname(__FILE__),config_file)
    if !File.exist?(config_file)
      log.fatal "File #{config_file} does not exist."
      exit 1
    end
    config = YAML.load(File.read(config_file))
    unless config.kind_of?(Hash)
      log.fatal "File #{config_file} is formatted incorrectly."
      exit 1
    end

    config['dry_run'] = dry_run == true ? true : false
    
    #Override the preamble if one was passed in on the cmdline
    config['preamble'] = preamble if !preamble.empty?
    #Override the job_creator_command if one was passed in on the cmdline
    config['job_creator_command'] = jcc if !jcc.empty?

    if !File.exists?(creds_file)
      warn "Credentials file '#{creds_file}' does not exist! Aborting."
      exit 1
    end

    credentials = YAML.load(File.read(creds_file))
    if !credentials.kind_of?(Hash)
      warn "Credentials file '#{creds_file}' is not properly formatted! Aborting."
      exit 1
    end
    
    EZQ::Job_Breaker.new(config,credentials,job,log)
  # Handle Ctrl-C gracefully
  rescue Interrupt
    warn "\nEZQ.Job_Breaker aborted!"
    exit 1
  end
end
