#!/usr/bin/env ruby

require 'bundler/setup'
require 'json'
require 'yaml'
require 'aws-sdk'
require 'logger'
require 'digest/md5'
require 'deep_merge'

module EZQ

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
    @queue = AWS::SQS.new.queues.named(@task_queue_name)
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
            @logger.info "Pushing #{bucket_comma_filename} on new thread"
            push_threads << Thread.new{ push_file( bucket_comma_filename, @dry_run, @credentials ) }
            @already_pushed << bucket_comma_filename
          end
        else
          msg = add_preamble(msg)
          enqueue_task(msg)
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
      msg = add_preamble(task.to_json)
      enqueue_task(msg)
      @enqueued_tasks.push(msg)
    end
  end


  protected
  # Enqueues a task, handling inline repeats if specified
  def enqueue_task(task)
    enqueue_task_impl(task)
    if @repeat_message_type == 'inline'
      @repeat_message_n_times.times {enqueue_task_impl(task)}
    end
  end
  
  
  protected
  # Does the true heavy-lifting of enqueueing a task
  def enqueue_task_impl(msg)
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
  def add_preamble(task)
    preamble = @preamble
    task_pa = YAML.load(task)
    if task_pa.kind_of?(Hash) && task_pa.has_key?('EZQ')
      pa = YAML.load(preamble)
      task_pa.deep_merge(pa)
      preamble = "#{task_pa.to_yaml}\n..."
    end
    return "#{preamble}\n#{task.sub(/-{3}\nEZQ.+?\.{3}\n/m,'')}"
  end
  
  
  protected
  # Pushes a file into S3. Argument should be string in form "bucket,filename"
  def push_file(bucket_comma_filename,dry_run,credentials)
    bname,fname = bucket_comma_filename.split(',').map{|s| s.strip}
    if dry_run
      puts "Would be pushing '#{fname}' into bucket '#{bname}'"
      return
    end
    if File.exists?(fname)
      s3 = AWS::S3.new(credentials)
      bucket = s3.buckets[bname]
      obj = bucket.objects.create(fname,Pathname.new(fname))
      AWS.config.http_handler.pool.empty!
    end
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
