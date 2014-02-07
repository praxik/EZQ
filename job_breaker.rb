#!/usr/bin/env ruby

require 'json'
require 'yaml'
require 'aws-sdk'
#require 'zlib'
#require 'base64'
require 'logger'
require 'digest/md5'

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
  # @param [String] job_string A JSON job string that will be broken up into
  #                 separate tasks and enqueued. This value will be ignored if
  #                 config::job_creator_command is set.
  # @param [Logger] logger The logger to use for logging internal output 
  def initialize(config,job_string = '',logger = nil)
    if !logger
      logger = Logger.new(STDOUT)
      logger.level = Logger::INFO
    end
    @logger = logger
    
    # Set up AWS with the specified credentials
    credentials = {}
    credentials['access_key_id'] = config['access_key_id']
    credentials['secret_access_key'] = config['secret_access_key']
    AWS.config(credentials)
    
    @job_creator_command = config['job_creator_command']
    @task_queue_name = config['task_queue_name']
    @preamble = config['preamble']
   
    get_queue
    if job_string.empty? && !@job_creator_command.empty?
      wrap_job_creator
    else  
      split_job(JSON.parse(job_string))
    end
    
  end
  
  
  
  protected
  # Call into AWS to get the result queue and put it into the variable 
  # @result_queue. No call to AWS is made if the result queue already exists
  # and has the name of the currently-specified queue.
  def get_queue
    @queue = AWS::SQS.new.queues.named(@job_queue_name)
    if !@queue.exists?
      @logger.fatal "No queue named #{@job_queue_name} exists."
      raise "No queue named #{@job_queue_name} exists."
    end
  end
  
  
  protected
  # Starts up the job_creator_command and listens to its STDOUT for tasks
  def wrap_job_creator
    IO.popen([@job_creator_command])  do |io| 
      while !io.eof?
        msg = io.gets
        if msg =~ /^push_file/
          push_file( msg.sub!(/^push_file\s*:\s*/,'') )
        else
          enqueue_task(msg)
        end
      end
    end
  end
  
  
  
  protected
  # Breaks up a job batch into individual tasks and enqueues then
  def split_job(json)
    json['tasks'].each do |job|
      enqueue_job(job.to_json)
    end
  end
  
  
  
  protected
  # Enqueues a task
  def enqueue_task(task)
    msg = "#{@preamble}\n#{task}"
    puts msg
    return
    digest = Digest::MD5.hexdigest(msg)
    sent = @queue.send_message(msg)
    if digest == sent.md5
      @logger.info "Enqueued msg #{sent.id} in queue '#{@task_queue_name}'"
    else
      @logger.error "Failed to enqueue msg:\n#{msg}\n-----------------------"
    end
  end
  
  
  
  protected
  # Pushes a file into S3. Argument should be string in form "bucket,filename"
  def push_file(bucket_comma_filename)
    bname, fname = bucket_comma_filename.split(',')
    if File.exists?(fname)
      s3 = AWS::S3.new
      bucket = s3.buckets[bname]
      obj = bucket.objects.create(fname,Pathname.new(fname))
    end
  end
  
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
  log_file = STDOUT
  op = OptionParser.new do |opts|
    opts.banner = "Usage: job_breaker.rb [options]"

    opts.on("-q", "--quiet", "Run quietly") do |q|
      quiet = q
    end
    opts.on("-c", "--config [CONFIG_FILE]", "Use configuration file CONFIG_FILE") do |file|
      config_file = file
    end
    opts.on("-l", "--log [LOG_FILE]","Log to file LOG_FILE") do |file|
      log_file = file
    end
    opts.on("-j", "--jobs [JOBS_FILE]","Read jobs from file JOBS_FILE") do |file|
      jobs_file = file
    end
    opts.on("-p", "--preamble [PREAMBLE]","Overrides the preamble set in the configuration file with contents of the string PREAMBLE") do |text|
      preamble = text
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
    
    #Override the preamble if one was passed in on the cmdline
    config['preamble'] = preamble if !preamble.empty?
    
    EZQ::Job_Breaker.new(config,job)
  # Handle Ctrl-C gracefully
  rescue Interrupt
    warn "\nEZQ.Job_Breaker aborted!"
    exit 1
  end
end
