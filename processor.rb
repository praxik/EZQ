#!/usr/bin/env ruby

require 'yaml'
require 'aws-sdk'
require 'zlib'
require 'base64'
require 'net/http'
require 'uri'
require 'logger'
require 'digest/md5'

require './x_queue'


module EZQ

class Processor


  # Create a processor based on a configuration hash
  # @param [Hash] config The configuration hash to use.
  def initialize(configuration,logger = nil)
    if !logger
      logger = Logger.new(STDOUT)
      logger.level = Logger::INFO
    end
    @logger = logger
    @logger.fatal "\n\n=============================================================="
    config = {}
    case configuration
    when Hash
      config = configuration
    when String
      config = parse_config_file(configuration)
    end
    # Set up AWS with the specified credentials
    credentials = {}
    credentials['access_key_id'] = config['access_key_id']
    credentials['secret_access_key'] = config['secret_access_key']
    AWS.config(credentials)

    # Populate instance variables
    @receive_queue_name = config['receive_queue_name']
    @receive_queue_type = config['recieve_queue_type']
    @store_message = config['store_message']
    @decompress_message = config['decompress_message']
    @process_command = config['process_command']
    @retry_on_failure = config['retry_on_failure']
    @retries = config['retries']
    @polling_options = config['polling_options']
    @result_step = config['result_step']
    @result_queue_name = config['result_queue_name']
    @result_queue_type = config['result_queue_type']
    @result_s3_bucket = config['result_s3_bucket']
    @compress_result = config['compress_result']
    @keep_trail = config['keep_trail']
    @cleanup_command = config['cleanup_command']
    @halt_instance_on_timeout = config['halt_instance_on_timeout']
    @smart_halt_when_idle_N_seconds = config['smart_halt_when_idle_N_seconds']
    @halt_type = config['halt_type']
    
    
    # Check for existence of receive queue
    @in_queue = AWS::SQS::X_queue.new(AWS::SQS.new.queues.named(@receive_queue_name).url)
    if !@in_queue.exists?
      @logger.fatal "No queue named #{@receive_queue_name} exists."
      raise "No queue named #{@receive_queue_name} exists."
    end
    
    # Get the result queue, if one was specified
    if @result_step == 'post_to_result_queue'
      @result_queue = AWS::SQS.new.queues.named(@result_queue_name)
      if !@in_queue.exists?
        @logger.fatal "No queue named #{@result_queue_name} exists."
        raise "No queue named #{@result_queue_name} exists."
      end
    end  
    
    # Are we running on an EC2 instance?
    @instance_id = ""
    @instance = nil
    @launch_time = nil
    uri = URI.parse("http://169.254.169.254/latest/meta-data/instance-id")
    begin
      @instance_id = Net::HTTP.get_response(uri).body
      @instance = AWS::EC2.new.instances[@instance_id]
      raise unless @instance.exists?#Using this pattern because either of the 
      # two previous calls can raise an exception, and I want to do exactly the
      # same thing in any of these cases.
      @launch_time = @instance.launch_time
      @logger.fatal "Running on EC2 instance #{@instance_id}"
    rescue
      @instance_id = ""
      @instance = nil
      @launch_time = nil
      @logger.fatal "Not running on an EC2 instance"
    end
  end
  
  
  
  # Parse a configuration file as yaml
  def parse_config_file(filename)
    @logger.info "Parsing configuration file #{filename}"
    config_file = File.join(File.dirname(__FILE__),filename)
    if !File.exist?(config_file)
      @logger.fatal "File #{filename} does not exist."
      raise "File #{filename} does not exist."
    end

    config = YAML.load(File.read(config_file))

    # Should probably do more thorough checking here....maybe using Kwalify?
    unless config.kind_of?(Hash)
      @logger.fatal "File #{filename} is formatted incorrectly."
      raise "File #{filename} is formatted incorrectly."
    end
    return config
  end
  
  
  
  # Decompresses the file and stores the result in a file with the same name.
  def decompress_file(filename)
    @logger.info "Decompressing file #{filename}"
    File.open(filename) do |cf|
      zi = Zlib::Inflate.new(Zlib::MAX_WBITS + 32)
      uncname = filename + '.uc'
      File.open(uncname, "w+") {|ucf|
      ucf << zi.inflate(cf.read) }
      zi.close
    end
    File.delete(filename)
    File.rename(filename + '.uc', filename)
  end
  
  
  
  # Compresses the file and stores the result in filename.gz
  def compress_file(filename)
    @logger.info "Compressing file #{filename}"
    Zlib::GzipWriter.open("#{filename}.gz",9) do |gz|
      gz.mtime = File.mtime(filename)
      gz.orig_name = filename
      gz.write IO.binread(filename)
    end
  end
  
  
  
  # Write the raw message body to the input file
  # @param [AWS::SQS::RecievedMessage] msg The message on which to operate
  # @param [String] infile The filename to which to write the message body 
  def get_raw(msg,infile)
    @logger.info 'Processing raw message'
    body = msg.body
    if @decompress_message
      @logger.info 'Decompressing message'
      zi = Zlib::Inflate.new(Zlib::MAX_WBITS + 32)
      body = zi.inflate(Base64.decode64(body))
      zi.close
    end
    File.open( "#{infile}", 'w' ) { |output| output << body }
  end
  


  # Parse the message body to find an s3 bucket and key, then download the
  # referenced object and store it as infile.
  # @param [AWS::SQS::RecievedMessage] msg The message on which to operate
  # @param [String] infile The filename in which to store the downloaded object
  def get_s3(msg,infile)
    body = YAML.load(msg.body)
    @logger.info "Getting object #{body['key']} from bucket #{body['bucket']}"
    s3 = AWS::S3.new
    bucket = s3.buckets[ body['bucket'] ]
    obj = bucket.objects[ body['key'] ]
    File.open(infile,'wb'){ |file| obj.read {|chunk| file.write(chunk)} }
    decompress_file(infile) if @decompress_message
  end



  # Parse the message body to find a uri, then download the referenced file and 
  # store it as infile.
  # @param [AWS::SQS::RecievedMessage] msg The message on which to operate
  # @param [String] infile The filename in which to store the downloaded object
  def get_uri(msg,infile)
    body = YAML.load(msg.body)
    @logger.info "Getting object at uri #{body['uri']}"
    uri = URI.parse(body['uri'])
    response = Net::HTTP.get_response(uri)
    File.open( "#{infile}", 'w' ) { |output| output << response.body }
    decompress_file(infile) if @decompress_message
  end



  # Replace a set of pre-defined tokens in str with appropriate strings
  # based on the message id.
  # @param [String] str The string containing tokens that need to be replaced
  # @param [String] input_filename The name of the input file for the current run
  # @param [String] id String version of the message id
  def expand_vars(str,input_filename,id)
    @logger.debug "Expanding vars in string '#{str}'"
    str = str.gsub('$full_msg_file', "#{id}.message")
    str = str.gsub('$input_file',input_filename)
    str = str.gsub('$id',id)
    @logger.debug "Expanded string: '#{str}'"
    str
  end
  
  
  
  # Runs the process_command on a job with id, on a given message and input file
  # @param [AWS::SQS::RecievedMessage] msg The message associated with this run
  # @param [String] input_filename String that will replace the $input_file token
  #                                in proces_command
  # @param [String] id Unique id associated with the current message
  def run_process_command(msg,input_filename,id)
    if @store_message
      # write the whole message and all its meta-data to arg2.message
      File.open( "#{id}.message", 'w' ) { |output| output << msg.to_yaml } 
    end
    commandline = expand_vars(@process_command,input_filename,id)
    @logger.info "Running command '#{commandline}'"
    
    success = system(commandline)
    if @retry_on_failure && !success
      num = @retries.to_i
      num.times do
        @logger.warn "Command '#{commandline}' failed; retrying"
        success = system(commandline)
        break if success
      end
    end  
    return success
  end
  
  

  # Perform cleanup operations on job associated with id and input_filename
  # @param [String] input_filename Name of input file for this job
  # @param [String] id ID of this job
  def cleanup(input_filename,id)
    @logger.info "Performing default cleanup for id #{id}"
    # Do other cleanup? Delete message files, other temporaries, etc?
    File.delete("output_#{id}.txt.gz") if File.exists?("output_#{id}.txt.gz")
    File.delete("output_#{id}.tar.gz") if File.exists?("output_#{id}.tar.gz")
    if !@keep_trail
      File.delete(input_filename) if File.exists?(input_filename)
      File.delete("#{id}.message") if File.exists?("#{id}.message")
      File.delete("output_#{id}.txt") if File.exists?("output_#{id}.txt")
      File.delete("output_#{id}.tar") if File.exists?("output_#{id}.tar")
    end

    cleaner = expand_vars(@cleanup_command,input_filename,id)
    if @cleanup_command != ""
      @logger.info "Performing custom cleanup with command '#{cleaner}'"
      system(cleaner)
    end
  end



  # Perform result_step
  def do_result_step
    @logger.debug 'Result step'
    case @result_step
    when 'none'
      return true
    when 'post_to_result_queue'
      return post_to_result_queue
    else
      raise "Invalid result_step: #{@result_step}. Must be one of [none,post_to_result_queue]"
      return false
    end
  end
  
  
  
  # Post a message to result_queue
  def post_to_result_queue
    msg = ""
    
    case @result_queue_type
    when 'raw'
      msg = make_raw_result
    when 's3'
      msg = make_s3_result
    end
    digest = Digest::MD5.hexdigest(msg)
    sent = @result_queue.send_message(msg)
    if digest == sent.md5
      @logger.info "Posted result message #{sent.id} to queue '#{@result_queue_name}'"
      return true
    else
      @logger.error "Failed to send result message for originating message #{@id}"
      return false
    end
  end
  
  
  
  #
  def make_raw_result
    @logger.info 'Forming message for raw result queue type'
    fname = "output_#{@id}.txt"
    body = {}
    body['processed_message_id'] = @id
    note = ""
    if File.exists?(fname)
      note = File.read(fname)
    else
      note = 'No output'
    end
    if @compress_result
      @logger.info 'Compressing raw message'
      note = Base64.encode64(Zlib::Deflate.deflate(note,9))
    end
    body['notes'] = note
    body.to_yaml
  end
  
  
  
  #
  def make_s3_result
    @logger.info 'Forming message for s3 result queue type'
    fname = "output_#{@id}.tar"
    body = {}
    body['processed_message_id'] = @id
    note = ""
    if File.exists?(fname)
      if @compress_result
        compress_file(fname)
        fname = "#{fname}.gz"
      end
      s3 = AWS::S3.new
      bucket = s3.buckets[@result_s3_bucket]
      obj = bucket.objects.create(fname,Pathname.new(fname))
      info = {'bucket'=>@result_s3_bucket,'key'=>fname}
      note = info.to_yaml
    else
      note = 'No output'
    end
    body['notes'] = note
    body.to_yaml
  end
  
  
  
  # Do the actual processing of a single message
  def process_message(msg)
    @logger.fatal '------------------------------------------'
    @logger .unknown "Received message #{msg.id}"
    @input_filename = msg.id + '.in'
    @id = msg.id
    
    case @receive_queue_type
    when 'raw'
      get_raw(msg,@input_filename)
    when 's3'
      get_s3(msg,@input_filename)
    when 'uri'
      get_uri(msg,@input_filename)
    else
      raise "Invalid receive_queue_type: #{receive_queue_type}. Must be one of [raw,s3,uri]."
    end 
    
    if run_process_command(msg,@input_filename,@id)
      # Do result_step before deleting the message in case result_step fails.
      if do_result_step()
        @logger.info "Processing successful. Deleting message #{@id}"
        msg.delete
      end
    end
    
    # Cleanup even if processing otherwise failed.
    cleanup(@input_filename,@id)
  end



  def poll_queue
    @in_queue.poll_no_delete(@polling_options) do |msg|
      if msg.is_a? Array
        msg.each {|item| process_message(item)}
      else
        process_message(msg)
      end
    end
  end


  # Start the main processing loop which requests messages from the queue,
  # pre-processes them according to queue type, passes that information to the 
  # chosen processing command, calls the result_step, and then cleans up.
  def start
    @logger.info "Starting queue polling"
    if @smart_halt_when_idle_N_seconds > 0 && @instance
      @polling_options[:idle_timeout] = @smart_halt_when_idle_N_seconds
      while (Time.now - @launch_time)/60%60 < 59 do
        poll_queue
      end
      halt_instance
    else
      poll_queue
      if @halt_on_timeout && @instance
        halt_instance 
      end
    end
  end
  
  
  
  # Halt this EC2 instance
  def halt_instance
    case @halt_type
    when 'stop'
      @instance.stop
    when 'terminate'
      @instance.terminate
    end
  end
  
  
  
end # class
end # module




################################################################################
# Run this bit if this file is being run directly as an executable rather than 
# being imported as a module.
if __FILE__ == $0
  require 'optparse'
  quiet = false
  config_file = 'queue_config.yml'
  log_file = STDOUT
  op = OptionParser.new do |opts|
    opts.banner = "Usage: processor.rb [options]"

    opts.on("-q", "--quiet", "Run quietly") do |q|
      quiet = q
    end
    opts.on("-c", "--config [CONFIG_FILE]", "Use configuration file CONFIG_FILE") do |file|
      config_file = file
    end
    opts.on("-l", "--log [LOG_FILE]","Log to file LOG_FILE") do |file|
      log_file = file
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

  begin
    puts "EZQ.Processor started.\n\n" unless quiet
    log = Logger.new(log_file)
    if quiet && log_file == STDOUT
      log.level = Logger::UNKNOWN
    else
      log.level = Logger::INFO
    end
    EZQ::Processor.new(config_file,log).start
  # Handle Ctrl-C gracefully
  rescue Interrupt
    warn "\nEZQ.Processor aborted!"
    exit 1
  end
end
################################################################################

