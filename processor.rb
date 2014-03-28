#!/usr/bin/env ruby

require 'bundler/setup'
require 'yaml'
require 'aws-sdk'
require 'zlib'
require 'base64'
require 'net/http'
require 'uri'
require 'logger'
require 'digest/md5'
require 'fileutils'

require './x_queue'


module EZQ

# Processor is a wrapper program which fetches messages from a queue, performs
# some light pre-processing on them, and then passes the result to the
# +process_command+ specified in the configuration. Processor can then take
# results from this processing and optionally post them to a +result_queue+.
class Processor

  protected
  # Set up a hash containing default values of everything we pulled from a
  # configuration file
  def make_var( name, default_value )
    @var_hash[ name ] = default_value
    instance_variable_set(name,default_value)
  end

  public
  # Create a processor based on a configuration hash
  #
  # @param [Hash or String] configuration The configuration hash to use. The full list of 
  #   configurable options is detailed in {file:Processor_Config_Details.md}.
  #   If configuration is a string, it will be interpreted as a file to open
  #   and parse as YAML.
  # @param [Hash] credentials The AWS credentials hash to use. It should contain
  #   two key-value pairs: access_key_id and secret_access_key_id. See
  #   http://aws.amazon.com/security-credentials for more information about the
  #   value of each key.
  # @param [Logger] logger The logger to use for logging internal output
  # @param [Hash] overrides Overrides anything specified in configuration with
  #   the values in this hash. This is particularly useful for keeping a base
  #   configuration file shared by multiple processes and overriding a small
  #   number of values for each instance.
  def initialize(configuration,credentials,logger = nil,overrides={})
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
    # Override config with values in overrides
    config.merge!(overrides)
    
    # Set up AWS with the specified credentials
    AWS.config(credentials)

    @pid = Process.pid
   
    # Create instance variables with sensible defaults
    @s3_files = []
    @s3_outs = []
    @uri_files = []
    @s3_endpoints = []
    @receive_queue_name = ''
    @error_queue_name = ''
    @polling_options = {:wait_time_seconds => 20}
    @halt_instance_on_timeout = false
    @smart_halt_when_idle_N_seconds = 0
    @halt_type = 'terminate'
    @store_message = false
    @decompress_message = false
    @process_command = ''
    @retry_on_failure = false
    @retries = 0
    @result_step = 'none'
    @result_queue_name = ''
    @compress_result_message = false
    @keep_trail = false
    @cleanup_command = ''
    @dont_hit_disk = false
    @msg_contents = ''

    # This will automatically create an instance variable based on each option
    # in the config. It will also populate each of our manually-
    # created ivars above with the default value as specified in the config.
    @var_hash = {}
    config.each { |k,v| make_var("@#{k}",v) }
    
    # Check for existence of receive queue
    @in_queue = AWS::SQS::X_queue.new(AWS::SQS.new.queues.named(@receive_queue_name).url)
    if !@in_queue.exists?
      @logger.fatal "No queue named #{@receive_queue_name} exists."
      raise "No queue named #{@receive_queue_name} exists."
    end
    
    # Get the result queue, if one was specified
    if @result_step == 'post_to_result_queue'
      get_result_queue
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
  
  
  protected
  # Call into AWS to get the result queue and put it into the variable 
  # +@result_queue+. No call to AWS is made if the result queue already exists
  # and has the name of the currently-specified queue.
  def get_result_queue
    return if @result_queue_name.empty?
    if (!@result_queue) || ( @result_queue && (@result_queue.arn.split(':').last != @result_queue_name) )
      @result_queue = AWS::SQS.new.queues.named(@result_queue_name)
    end
    if !@result_queue.exists?
      @logger.fatal "No queue named #{@result_queue_name} exists."
      raise "No queue named #{@result_queue_name} exists."
    end
  end
  
  
  
  protected
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
  
  
  
  protected
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
  
  
  
  protected
  # Compresses the file and stores the result in filename.gz
  def compress_file(filename)
    @logger.info "Compressing file #{filename}"
    Zlib::GzipWriter.open("#{filename}.gz",9) do |gz|
      gz.mtime = File.mtime(filename)
      gz.orig_name = filename
      gz.write IO.binread(filename)
    end
  end


  protected
  # Replace a set of pre-defined tokens in str with appropriate strings
  # based on the message id.
  # @param [String] str The string containing tokens that need to be replaced
  # @param [String] input_filename The name of the input file for the current run
  # @param [String] id String version of the message id
  def expand_vars(str,input_filename,id)
    @logger.debug "Expanding vars in string '#{str}'"
    strc = str.clone
    strc.gsub!('$full_msg_file', "#{id}.message")
    strc.gsub!('$input_file',input_filename)
    @s3_files.each_with_index { |file,idx| strc.gsub!("$s3_#{idx + 1}",file) }
    @uri_files.each_with_index { |file,idx| strc.gsub!("$uri_#{idx + 1}",file) }
    strc.gsub!('$id',id)
    strc.gsub!('$pid',@pid.to_s)
    strc.gsub!('$msg_contents',@msg_contents.dump) #Have to escape msg conts!
    @logger.debug "Expanded string: '#{strc}'"
    return strc
  end
  
  
  protected
  # Save the entire message in case processors need access to meta information
  # @param [AWS::SQS::ReceivedMessage] msg The message on which to operate
  # @param [String] id The id of this message to use for forming filename
  def save_message(msg,id)
    tmp = {}
    tmp['body'] = msg.body
    tmp['sender_id'] = msg.sender_id
    tmp['sent_at'] = msg.sent_at
    tmp['receive_count'] = msg.receive_count
    tmp['first_received_at'] = msg.first_received_at
    File.open( "#{id}.message", 'w' ) { |output| output << tmp.to_yaml }  
  end
  
  
  protected
  # Runs the process_command on a job with id, on a given message and input file
  # @param [AWS::SQS::ReceivedMessage] msg The message associated with this run
  # @param [String] input_filename String that will replace the $input_file 
  # token in process_command
  # @param [String] id Unique id associated with the current message
  def run_process_command(msg,input_filename,id)
    save_message(msg,id) if @store_message
    commandline = expand_vars(@process_command,input_filename,id)
    @logger.info "Running command '#{commandline}'"
    success = system(commandline)
    @logger.fatal "Command does not exist!" if success == nil
    @logger.warn "Command '#{commandline}' failed" if !success
    if @retry_on_failure && !success
      num = @retries.to_i
      num.times do
        @logger.warn "Command '#{commandline}' failed; retrying"
        success = system(commandline)
        break if success
      end
    end
    if !success
      err_msg = "Command '#{commandline}' failed under pid #{@pid.to_s} "
      err_msg += "on instance #{@instance_id} " if !@instance_id .empty?
      err_msg += "with message contents:\n\n"
      err_msg += @msg_contents
      send_error(err_msg) unless success
    end
    return success
  end


  protected
  # Sends an error message to the named error queue
  # @param [String] msg The message to put in the error queue
  def send_error( msg )
    return if @error_queue_name.empty?
    err_q = AWS::SQS.new.queues.named(@error_queue_name)
    if !err_q.exists?
      @logger.error "Unable to connect to error queue #{@error_queue_name}."
      return
    end
    err_q.send_message( msg )
  end
  
  
  protected
  # Resets to the default configuration specified in the config file parsed at
  # startup, unless the default value has been permanently changed
  def reset_configuration
    @var_hash.each {|k,v| instance_variable_set(k,v) }
  end
  
  
  protected
  # Parse a message body to override settings for this one message. Some
  # settings cannot be overridden:
  # * 'receive_queue_name'
  # * 'polling_options'
  # * 'smart_halt_when_idle_N_seconds'
  # Other overrides are permanent, since there is no logical interpretation of 
  # "only for this message":
  # * 'halt_instance_on_timeout'
  # * 'halt_type'
  def override_configuration( body )
    @logger.debug "override_configuration"
    protected_vars = ['receive_queue_name','polling_options','smart_halt_when_idle_N_seconds','get_files']
    change_default_vars = ['halt_instance_on_timeout','halt_type']
    cfg = YAML.load(body)
    return if !cfg.kind_of? Hash
    cfg = cfg['EZQ']
    return if !cfg
    cfg.each do |k,v|
      if @var_hash.has_key? "@#{k}"
        instance_variable_set("@#{k}",v) unless protected_vars.include?(k)
        @var_hash["@#{k}"] = v if change_default_vars.include?(k)
        @logger.info "overrode config for #{k}"
      end
    end
  end


  protected
  # Perform cleanup operations on job associated with id and input_filename
  #
  # @param [String] input_filename Name of input file for this job
  # @param [String] id ID of this job
  def cleanup(input_filename,id)
    @logger.info "Performing default cleanup for id #{id}"
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
    reset_configuration
  end


  protected
  # Perform result_step
  def do_result_step
    @logger.debug 'Result step'
    put_s3_files
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
  
  
  protected
  # Post a message to result_queue
  def post_to_result_queue
    msg = ""
    preamble = {'EZQ'=>{}}
    body = make_raw_result(preamble)
    preamble['EZQ']['processed_message_id'] = @id
    preamble['EZQ']['get_s3_files'] = @s3_outs if !@s3_outs.empty?
    msg = "#{preamble.to_yaml}...\n#{body}"
    digest = Digest::MD5.hexdigest(msg)
    get_result_queue
    sent = @result_queue.send_message(msg)
    if digest == sent.md5
      @logger.info "Posted result message #{sent.id} to queue '#{@result_queue_name}'"
      return true
    else
      @logger.error "Failed to send result message for originating message #{@id}"
      return false
    end
  end
  
  
  protected
  # Pull the contents of the output file into the result message.
  def make_raw_result(preamble)
    fname = "output_#{@id}.txt"
    body = ""
    if File.exists?(fname)
      body = File.read(fname)
      # Pull out any preamble set directly in the body.
      if !body.empty?
        by = YAML.load(body)
        if by.kind_of?(Hash) && by.has_key?('EZQ')
          preamble.merge!(by)
          body.sub!(/-{3}\nEZQ.+?\.{3}\n/m,'')
        end
      end
    else
      body = 'No output'
    end
    if @compress_result_message
      @logger.info 'Compressing raw message'
      body = Base64.encode64(Zlib::Deflate.deflate(body,9))
    end
    return body
  end
  


  protected
  # Send any specified results files to s3 endpoints,
  def put_s3_files
    @s3_outs.clear
    @s3_endpoints.each do |ep|
      fname = ep['filename']
      if File.exists?(fname)
        s3 = AWS::S3.new
        bucket = s3.buckets[ep['bucket']]
        obj = bucket.objects.create(ep['key'],Pathname.new(fname))
        info = {'bucket'=>ep['bucket'],'key'=>ep['key']}
        @s3_outs.push(info)
      end
    end
  end
  
  
  protected
  # Strips out the message preamble containing explicit EZQ directives
  def strip_directives(msg)
	msg.body.sub!(/-{3}\nEZQ.+?\.{3}\n/m,'')
  end


  protected
  def fetch_s3(msg)
    @s3_files.clear
    body = YAML.load(msg.body)
    return true if !body.kind_of?(Hash)
    return true if !body.has_key?('EZQ')
    preamble = body['EZQ']
    return true if !preamble.has_key?('get_s3_files')
    files = preamble['get_s3_files']
    files.each do |props|
      @logger.info "Getting object #{props['key']} from bucket #{props['bucket']}"
      @s3_files.push(props['key'])
      s3 = AWS::S3.new
      bucket = s3.buckets[ props['bucket'] ]
      obj = bucket.objects[ props['key'] ]
      FileUtils.mkdir_p(File.dirname(props['key']))
      begin
        File.open(props['key'],'wb'){ |f| obj.read {|chunk| f.write(chunk)} }
        # TODO:
        # Perhaps I'll reinstate decompression ability via another file-specific
        # k-v pair:  decompress: true/false
        # Really need to support something more than zlib for this to be useful
        #decompress_file(infile) if @decompress_message
      rescue
        @logger.error "Unable to fetch #{props['key']} from S3."
        return false  
      end
    end
    return true
  end


  protected
  def fetch_uri(msg)
    @uri_files.clear
    body = YAML.load(msg.body)
    return nil if !body.kind_of?(Hash)
    return nil if !body.has_key?('EZQ')
    preamble = body['EZQ']
    return nil if !preamble.has_key?('get_uri_contents')
    files = preamble['get_uri_contents']
    files.each_with_index do |props,idx|
      @logger.info "Getting object at uri #{props['uri']}"
      save_name = props.has_key?('save_name') ? props['save_name'] : "uri#{idx+1}"
      @uri_files.push(save_name)
      uri = URI.parse(props['uri'])
      response = Net::HTTP.get_response(uri)
      File.open( "#{save_name}", 'w' ) { |output| output << response.body }
      # see TODO in fetch_s3
      #decompress_file(infile) if @decompress_message
    end
  end


  protected
  def store_s3_endpoints(msg)
    @s3_endpoints.clear
    body = YAML.load(msg.body)
    return nil if !body.kind_of?(Hash)
    return nil if !body.has_key?('EZQ')
    preamble = body['EZQ']
    @s3_endpoints = preamble['put_s3_files'] if preamble.has_key?('put_s3_files')
    return nil
  end
  
  protected
  # Do the actual processing of a single message
  def process_message(msg)
    @logger.fatal '------------------------------------------'
    @logger.fatal "Received message #{msg.id}"
    @input_filename = msg.id + '.in'
    @id = msg.id
    
    override_configuration(msg.body)
    if !fetch_s3(msg)
      cleanup(@input_filename,@id)
      return
    end
    fetch_uri(msg)
    store_s3_endpoints(msg)
    strip_directives(msg)
    
    body = msg.body
    if !body.empty? && @decompress_message
      @logger.info 'Decompressing message'
      zi = Zlib::Inflate.new(Zlib::MAX_WBITS + 32)
      body = zi.inflate(Base64.decode64(body))
      zi.close
    end
    @msg_contents = body
    File.open( "#{@input_filename}", 'w' ) { |output| output << body } unless @dont_hit_disk
    
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


  protected
  # Poll the in_queue, handling arrays of messages appropriately
  def poll_queue
    @in_queue.poll_no_delete(@polling_options) do |msg|
      Array(msg).each {|item| process_message(item)}
    end
  end



  protected
  # Halt this EC2 instance
  def halt_instance
    case @halt_type
    when 'stop'
      @instance.stop
    when 'terminate'
      @instance.terminate
    end
  end



  public
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
  
    
  
  
end # class
end # module



################################################################################
# Run this bit if this file is being run directly as an executable rather than 
# being imported as a module.
if __FILE__ == $0
  require 'optparse'
  quiet = false
  config_file = 'queue_config.yml'
  creds_file = 'credentials.yml'
  queue = nil
  log_file = STDOUT
  op = OptionParser.new do |opts|
    opts.banner = "Usage: processor.rb [options]"

    opts.on("-q", "--quiet", "Run quietly") do |q|
      quiet = q
    end
    opts.on("-c", "--config [CONFIG_FILE]", "Use configuration file CONFIG_FILE. Defaults to queue_config.yml if not specified.") do |file|
      config_file = file
    end
    opts.on("-l", "--log [LOG_FILE]","Log to file LOG_FILE") do |file|
      log_file = file
    end
    opts.on("-r", "--credentials [CREDS_FILE]","Use credentials file CREDS_FILE. Defaults to credentials.yml if not specified.") do |file|
      creds_file = file
    end
    opts.on("-q", "--queue [QUEUE_NAME]","Poll QUEUE_NAME for tasks rather than the receive_queue specified in the config file") do |q|
      queue = q
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
    if log_file != STDOUT
      lf = File.new(log_file, 'a')
      log = Logger.new(lf)
      $stderr = lf
    else
      log = Logger.new(log_file)
    end
    if quiet && log_file == STDOUT
      log.level = Logger::UNKNOWN
    else
      log.level = Logger::INFO
    end

    if !File.exists?(creds_file)
      warn "Credentials file '#{creds_file}' does not exist! Aborting."
      exit 1
    end

    credentials = YAML.load(File.read(creds_file))
    if !credentials.kind_of?(Hash)
      warn "Credentials file '#{creds_file}' is not properly formatted! Aborting."
      exit 1
    end

    overrides = queue ? {"receive_queue_name"=>queue} : {}
    EZQ::Processor.new(config_file,credentials,log,overrides).start
  # Handle Ctrl-C gracefully
  rescue Interrupt
    warn "\nEZQ.Processor aborted!"
    exit 1
  end
end
################################################################################



