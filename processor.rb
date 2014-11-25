#!/usr/bin/env ruby

require 'bundler/setup'
require 'yaml'
require 'aws-sdk'
require 'zlib'
require 'base64'
require 'net/http'
require 'uri'
require 'digest/md5'
require 'fileutils'
require 'zip'
require 'socket'

require './x_queue'
require_relative './ezqlib'
require_relative './dual_log'


module EZQ

  # Processor is a wrapper program which fetches messages from a queue, performs
  # some light pre-processing on them, and then passes the result to the
  # +process_command+ specified in the configuration. Processor can then take
  # results from this processing and optionally post them to a +result_queue+.
  class Processor

    protected
    def open_exit_port(port)
      @logger.info "Opening exit port #{port}"
      Thread.new do
        server = TCPServer.new(port)
        while @run do
          client = server.accept
          @logger.info "Got exit port connection"
          val = client.gets
          @logger.info "Received '#{val}' on exit port"
          if  val.chop == 'TERMINATE'
            @run = false
            @logger.info "Caught TERMINATE"
          end
          client.close
        end
      end
    end

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

      # Create a bunch of instance variables and give them sensible defaults
      set_defaults()

      # Create an instance variable based on each option in the config, and
      # populate each of our manually-created ivars from set_defaults with
      # the value specified in the config.
      config.each { |k,v| make_var("@#{k}",v) }

      # Grab the receive_queue
      init_receive_queue()

      # Get the result queue, if one was specified
      if @result_step == 'post_to_result_queue'
        init_result_queue()
      end

      set_instance_details()

      @run = true
      # Setup to get graceful exit signal
      # For *nix:
      Signal.trap('SIGTERM'){@run = false; @logger.info "Caught SIGTERM"}
      # For Windows:
      open_exit_port(config.fetch('exit_port',8642)) if RUBY_PLATFORM =~ /mswin|mingw/
    end


    protected
    def set_defaults
      @pid = Process.pid

      # Create instance variables with sensible defaults
      @s3_files = []
      @s3_outs = []
      @uri_files = []
      @s3_endpoints = []
      @receive_queue_name = ''
      @error_queue_name = 'errors'
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
      @result_overflow_bucket = ''
      @file_as_body = nil # This gets set only when we see a get_s3_file_as_body
      # directive
      @collect_errors_command = ''
      @atomicity = 1

      @var_hash = {}

      @instance_id = ""
      @instance = nil
      @launch_time = nil
    end


    protected
    def set_instance_details
      # Are we running on an EC2 instance?
      uri = URI.parse("http://169.254.169.254/latest/meta-data/instance-id")
      @instance_id = Net::HTTP.get_response(uri).body
      @instance = AWS::EC2.new.instances[@instance_id]
      raise unless @instance.exists? # Either of the two previous calls can raise
      # an exception, and we want to do the same thing in either case.
      @launch_time = @instance.launch_time
      @logger.unknown "Running on EC2 instance #{@instance_id}"
    rescue
      @instance_id = ""
      @instance = nil
      @launch_time = nil
      @logger.unknown "Not running on an EC2 instance"
    end



    protected
    # Grabs the receive_queue set up in the configuration and stores it in
    # instance var @in_queue. If the queue doesn't exist, we try to create it.
    def init_receive_queue
      @in_queue = EZQ.exceptional_retry_with_backoff(4) do
        AWS::SQS::X_queue.new(EZQ.get_queue(@receive_queue_name,true).url)
      end
      if !@in_queue.exists?
        m = "Exception: No queue named #{@receive_queue_name} exists."
        @logger.fatal m
        send_error(m,true)
      end
    end


    protected
    # Call into AWS to get the result queue and put it into the variable
    # +@result_queue+. No call to AWS is made if the result queue already exists
    # and has the name of the currently-specified queue.
    def init_result_queue
      return nil if @result_queue_name.empty?
      if (!@result_queue) || ( @result_queue && (@result_queue.arn.split(':').last != @result_queue_name) )
        @result_queue = EZQ.exceptional_retry_with_backoff(3) do
          EZQ.get_queue(@result_queue_name,true)
        end
      end
      if !@result_queue.exists?
        m = "Exception: No queue named #{@result_queue_name} exists."
        @logger.fatal m
        send_error(m, true)
      end
      return nil
    end



    protected
    # Parse a configuration file as yaml
    def parse_config_file(filename)
      @logger.info "Parsing configuration file #{filename}"
      config_file = File.join(File.dirname(__FILE__),filename)
      if !File.exist?(config_file)
        m = "Exception: Configuration file #{filename} does not exist."
        @logger.fatal m
        send_error(m, true)
      end

      config = YAML.load(File.read(config_file))

      # Should probably do more thorough checking here....maybe using Kwalify?
      unless config.kind_of?(Hash)
        m = "Exception: Config file #{filename} is formatted incorrectly."
        @logger.fatal m
        send_error(m, true)
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
      tmp['body'] = @msg_contents # Instead of msg.body because by now we will
      # have decompressed, pulled body out of S3,
      # and mutated the body in other ways.
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
    def run_process_command(input_filename,id,log_command=true)
      commandline = expand_vars(@process_command,input_filename,id)
      @logger.info "Running command '#{commandline}'" if log_command
      @logger.info 'Running process_command' if !log_command

      success = false
      output = []
      tries = @retry_on_failure ? @retries.to_i + 1 : 1
      tries.times do
        success, output = exec_cmd_thread(commandline)
        break if success
      end

      @logger.fatal "Command does not exist!" if success == nil
      @logger.error "Command '#{commandline}' failed with output: \n#{output.join("\n")}" if !success
      if !success
        err_hash = {}
        # Everything in here that could potentially be large has to be forcibly
        # capped to ensure we don't go over the 256kB message size limit
        err_hash['stdout_stderr'] = output.join("\n").byteslice(0..99999) # limit to 100 kB
        err_hash['error_collection'] = collect_errors(input_filename,id).byteslice(0..99999) # limit to 100kB
        err_hash['command'] = commandline.byteslice(0..2999) # limit to 3kB
        err_hash['msg_id'] = @id
        err_hash['input'] = @msg_contents.byteslice(0..49999) # limit to 50kB
        err_hash['pid'] = @pid
        err_hash['instance'] = @instance_id if !@instance_id.empty?
        send_error(err_hash.to_yaml)
      end

      return success
    end


    protected
    # Runs EZQ.exec_cmd in a separate thread, allowing us to check the @run flag
    # at intervals and abort if a graceful exit has been requested.
    def exec_cmd_thread(cmd)
      thr = Thread.new do
        Thread.current[:retval] = [false,'Process_command killed in graceful exit']
        Thread.current[:retval] = EZQ.exec_cmd(cmd)
      end

      loop do
        break if thr.join(5)
        thr.kill if !@run
      end

      return thr[:retval]
    end


    protected
    # Sends an error message to the named error queue
    # @param [String] msg The message to put in the error queue
    # @param [Bool] failout Whether to raise an exception
    def send_error( msg, failout=false )
      if @error_queue_name.empty?
        if failout
          raise msg
        else
          return
        end
      end

      err_msg = {'timestamp' => Time.now.strftime('%F %T %N'), 'error' => msg}
      EZQ.enqueue_message(err_msg.to_yaml,{},@error_queue_name,true)
      raise msg if failout
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
      # Result queue may have changed. This does nothing if it hasn't.
      init_result_queue()
    rescue
      @logger.info "No overrides."
    end


    protected
    # Perform cleanup operations on job associated with id and input_filename
    #
    # @param [String] input_filename Name of input file for this job
    # @param [String] id ID of this job
    def cleanup(input_filename,id)
      @logger.info "Performing default cleanup for id #{id}"
      @file_as_body = nil
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
    # Runs collect_errors_command using input_filename and id.
    def collect_errors(input_filename,id)
      errors = []
      if @collect_errors_command
        command = expand_vars(@collect_errors_command,input_filename,id)
        return '' if command.empty?
        begin
          IO.popen(command) do |io|
            while !io.eof?
              errors << io.gets
            end
          end
        rescue
          errors << "collect_errors_command '#{command}' does not exist."
        end
      end
      return errors.join("\n")
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
        m = "Invalid result_step: #{@result_step}. Must be one of [none,post_to_result_queue]"
        @logger.error m
        send_error(m)
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
      # Non-destuctively add our s3 files to any that were previously set
      preamble['EZQ']['get_s3_files'] = Array(preamble['EZQ']['get_s3_files']) +
      @s3_outs if !@s3_outs.empty?
      digest = EZQ.enqueue_message(body,preamble,@result_queue,false,
      bucket = 'EZQOverflow.praxik')
      if !digest.empty?
        @logger.info "Posted result message to queue '#{@result_queue_name}'"
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
        # Pull out any preamble set directly in the body, merge it into the
        # standard preamble, then remove the preamble from the body.
        if !body.empty? and body =~ /-{3}\nEZQ/
          begin
            by = YAML.load(body)
            if by.kind_of?(Hash) && by.has_key?('EZQ')
              preamble.merge!(by)
              body.sub!(/-{3}\nEZQ.+?\.{3}\n/m,'')
            end
          rescue
            # This is triggered on a failed YAML load. Carry on; the body might
            # not be valid YAML, and that's okay.
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
          AWS.config.http_handler.pool.empty! # Hack to solve odd timeout issue
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
    # Fetches *all* the files mentioned in get_s3_files directive in EZQ preamble
    def fetch_s3(msgbody)
      @s3_files.clear
      body = YAML.load(msgbody)
      return true if !body.kind_of?(Hash)
      return true if !body.has_key?('EZQ')
      preamble = body['EZQ']
      return true if !preamble

      return false if !get_s3_file_as_body(preamble)

      return true if !preamble.has_key?('get_s3_files')
      files = preamble['get_s3_files']
      files.each do |props|
        return false if !get_s3_file(props['bucket'],props['key'],msgbody)
        if props.has_key?('decompress') and props['decompress'] == true
          Zip.on_exists_proc = true # Don't raise if extracted files already exist
          Zip::File.open(props['key']) do |zip_file|
            # Don't actually overwrite an existing file, because doing so with
            # multiple processes simultaneously -- it happens! -- causes crashes.
            zip_file.each { |entry| entry.extract(entry.name) if !File.exists?(entry.name) }
          end
        end
      end

      return true
    end


    protected
    # Determines whether preamble indicates a file_as_body, and retrieves the file
    # if so. Returns false if there were errors; true otherwise. Sets value of
    # @file_as_body as a side effect.
    def get_s3_file_as_body(preamble)
      @file_as_body = nil
      if preamble.has_key?('get_s3_file_as_body')
        info = preamble['get_s3_file_as_body']
        bucket = info['bucket']
        key = info['key']
        @logger.info "Getting file as message body"
        return false if !get_s3_file(bucket,key)
        @file_as_body = "#{bucket},#{key}"
      end
      return true
    end


    protected
    # Pulls a single file down from S3
    # Last parameter is so we can pass extra info that goes into an error message
    # if the file can't be downloaded.
    def get_s3_file(bucket,key,msgbody = 'no input given')
      @logger.info "Getting object #{key} from bucket #{bucket}"
      @s3_files << key
      if !EZQ.get_s3_file(bucket,key)
        issue = "Unable to fetch #{key} from S3."
        @logger.error(issue)
        err_hash = {}
        err_hash['issue'] = issue
        err_hash['msg_id'] = @id
        err_hash['input'] = msgbody.byteslice(0..49999)
        err_hash['pid'] = @pid
        err_hash['instance'] = @instance_id if !@instance_id.empty?
        send_error(err_hash.to_yaml,false)
        return false
      end
      return true
    end


    protected
    def fetch_uri(msgbody)
      @uri_files.clear
      body = YAML.load(msgbody)
      return nil if !body.kind_of?(Hash)
      return nil if !body.has_key?('EZQ')
      preamble = body['EZQ']
      return nil if !preamble
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
    def store_s3_endpoints(msgbody)
      @s3_endpoints.clear
      body = YAML.load(msgbody)
      return nil if !body.kind_of?(Hash)
      return nil if !body.has_key?('EZQ')
      preamble = body['EZQ']
      return nil if !preamble
      @s3_endpoints = preamble['put_s3_files'] if preamble.has_key?('put_s3_files')
      return nil
    end


    protected
    def delete_file_as_body(bucket_comma_file)
      return nil if !bucket_comma_file # Why did this even get called?
      bucket,key = bucket_comma_file.split(',')
      s3 = AWS::S3.new
      b = s3.buckets[ bucket ]
      obj = b.objects[ key ] if b
      begin
        obj.delete if obj
      rescue
        @logger.warn "Failed to delete file as body #{bucket_comma_file}"
      end
      File.delete(key) if File.exists?(key)
      return nil
    end


    protected
    # Do the actual processing of a single message
    def process_message(msg)
      # Just re-vis the message if graceful exit was requested
      if !@run
        set_visibility(msg,10)
        return false
      end

      @logger.unknown "-----------------Received message #{msg.id}-------------------------"
      @input_filename = msg.id + '.in'
      @id = msg.id

      override_configuration(msg.body)
      if !fetch_s3(msg.body)
        cleanup(@input_filename,@id)
        return false
      end
      fetch_uri(msg.body)
      store_s3_endpoints(msg.body)
      strip_directives(msg)

      ## Split this out into separate method
      # Message "body" will be either the actual body from the queue message,
      # sans preamble (the usual case) or the contents of the special
      # file_as_body that was pulled from S3 in the event that the message body
      # was too big to fit in SQS. @file_as_body is formatted as "bucket,key".
      body = @file_as_body != nil ? File.read(@file_as_body.split(',')[1]) : msg.body
      if !body.empty? && @decompress_message
        @logger.info 'Decompressing message'
        zi = Zlib::Inflate.new(Zlib::MAX_WBITS + 32)
        body = zi.inflate(Base64.decode64(body))
        zi.close
      end
      ##
      @msg_contents = body
      File.open( "#{@input_filename}", 'w' ) { |output| output << body } unless @dont_hit_disk
      save_message(msg,@id) if @store_message
      success = run_process_command(@input_filename,@id)
      if success
        # Do result_step before deleting the message in case result_step fails.
        if do_result_step()
          @logger.info "Processing successful. Deleting message #{@id}"
          delete_message(msg)
          delete_file_as_body(@file_as_body) if @file_as_body # This must stay linked to deleting
          # message from queue
        else
          # Since we've failed, make the message visible again in 10 seconds
          set_visibility(msg,10)
        end
      else
        set_visibility(msg,10)
      end

      # Cleanup even if processing otherwise failed.
      cleanup(@input_filename,@id)
      return success
    end


    protected
    def set_visibility(msg,timeout)
      begin
        msg.visibility_timeout = timeout
      rescue
        @logger.warn "Failed to reset timeout on msg #{msg.id}"
      end
    end


    protected
    def delete_message(msg)
      begin
        msg.delete
      rescue
        @logger.warn "Failed to delete msg #{msg.id}"
      end
    end


    protected
    # Do the actual processing of a message molecule
    def process_molecule(mol)
      # Just re-vis the molecule if graceful exit was requested
      if !@run
        mol.each{|msg| set_visibility(msg,10)}
        return false
      end

      @logger.unknown "-------------------Received molecule of #{mol.size} messages-----------------------"
      @id = mol[0].id  # Use id of first message as the id for the entire op
      @input_filename = @id + '.in'

      preambles = mol.map{|msg| EZQ.extract_preamble(msg.body)}
      # @file_as_body will be nil if one wasn't specified or if there were errors
      # getting it
      body_files = preambles.map{|pre| get_s3_file_as_body(pre); @file_as_body}
      uni_pre = preambles.reduce(&:merge)
      uni_pre.delete('get_s3_file_as_body')
      tmp = uni_pre
      uni_pre = {}
      uni_pre['EZQ'] = tmp

      override_configuration(uni_pre.to_yaml)

      if !fetch_s3(uni_pre.to_yaml)
        #mol.each{|msg| cleanup(@input_filename,msg.id)}
        return false
      end
      fetch_uri(uni_pre.to_yaml)
      store_s3_endpoints(uni_pre.to_yaml)

      #mol.each{|msg| strip_directives(msg)}
      mol.each_with_index do |msg,idx|
        strip_directives(msg)
        if body_files[idx] != nil
          begin
            contents = File.read(body_files[idx].split(',')[1])
          rescue
            @logger.error "process_molecule couldn't read file #{body_files[idx].split(',')[1]}"
            return false
          end
          msg.body.sub!(/.*/,contents)
        end
      end

      # FIXME: We're ignoring decompression here.

      # Concatenate all the message bodies into one big one.
      body = mol.map{|msg| msg.body}.join("\n#####!@$$@!#####\n")
      @msg_contents = body
      File.open( "#{@input_filename}", 'w' ) { |output| output << body } unless @dont_hit_disk

      # Just save the first message since we have no concept of saving all of
      # them. Since save_message uses @msg_contents for the body, this is kind
      # of okay.
      save_message(msg,id) if @store_message

      success = run_process_command(@input_filename,@id)
      if success
        # Do result_step before deleting the message in case result_step fails.
        if do_result_step()
          @logger.info "Processing successful. Deleting molecule."
          mol.each{|msg| delete_message(msg)}
          # body files are effectively part of the message
          body_files.delete_if{|f| f == nil}
          body_files.each{|f| delete_file_as_body(f)}
        else
          # Make the message visible again in 10 seconds, rather than whatever its
          # natural timeout is
          mol.each{|msg| set_visibility(msg,10)}
        end
      end

      # Cleanup even if processing otherwise failed.
      cleanup(@input_filename,@id)
      return success
    end


    protected
    # Poll the in_queue, handling arrays of messages appropriately
    def poll_queue
      if @atomicity > 1
        poll_queue_atom(@atomicity)
        return nil
      end
      @in_queue.poll_no_delete(@polling_options) do |msg|
        msgary = Array(msg)
        msgary.each {|item| process_message(item)}
        exit if !@run
      end
      return nil
    end


    protected
    def poll_queue_atom(a)
      opts = atom_opts(a)
      msgs = []
      while @run
        msgs += Array(@in_queue.receive_message(opts))
        if msgs.size < a
          n = a - msgs.size
          opts[:limit] = n > 10 ? 10 : n
          next
        end
        msgs = remove_duplicates(msgs)
        msgs.each_slice(a) do |molecule|
          if molecule.size == a
            process_molecule(molecule)
            msgs -= molecule
          end
        end
        # At this point, msgs may still contain fewer than 'a' entries. Don't do
        # anything that might clobber them! The idea is to continue polling for
        # messages until we get enough to form a molecule.
      end
    end


    protected
    # Removes (and deletes from queue!) any message duplicates and returns a new
    # array containing only unique messages.
    def remove_duplicates(msgs)
      keep = []
      tail = msgs.clone()
      while tail.size > 0
        keep << tail.shift
        tail.delete_if{|m| m.body == keep.last.body ? (m.delete(); true) : false}
      end
      return keep
    end


    protected
    # Sets up an options hash for atomic polling.
    def atom_opts(a)
      opts = {}
      opts[:limit] = a > 10 ? 10 : a
      opts[:attributes] = @polling_options.fetch('attributes',nil)
      opts[:wait_time_seconds] = @polling_options.fetch('wait_time_seconds',20)
      return opts
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
        while ((Time.now - @launch_time) % 3600) < (3600-@smart_halt_when_idle_N_seconds) do
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
  severity = 'info'
  queue = nil
  error_queue = nil
  log_file = STDOUT
  loggly_token = nil
  loggly_severity = 'info'
  app_name = 'EZQ::Processor'
  exit_port = '8642'
  debug_port = nil
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
    opts.on("-s", "--log_severity [SEVERITY]","Set local log severity to one of: unknown, fatal, error, warn, info, debug. Default = info") do |s|
      severity = s
    end
    opts.on("-t", "--token [LOGGLY_TOKEN]","Loggly token. Turns on logging to loggly") do |token|
      loggly_token = token
    end
    opts.on("-u", "--loggly_severity [SEVERITY]","Set loggly severity to one of: unknown, fatal, error, warn, info, debug. Default = info") do |s|
      loggly_severity = s
    end
    opts.on("-n", "--app_name [APP_NAME]","Application name to use with Loggly.") do |name|
      app_name = name
    end
    opts.on("-r", "--credentials [CREDS_FILE]","Use credentials file CREDS_FILE. Defaults to credentials.yml if not specified.") do |file|
      creds_file = file
    end
    opts.on("-Q", "--queue [QUEUE_NAME]","Poll QUEUE_NAME for tasks rather than the receive_queue specified in the config file") do |q|
      queue = q
    end
    opts.on("-e", "--error_queue [QUEUE_NAME]","Report fatal errors to this queue rather than the error_queue specified in the config file") do |q|
      error_queue = q
    end
    opts.on("-p", "--exit_port [PORT]","On MSWindows, open a socket on PORT to listen for 'TERMINATE' message for graceful exit") do |p|
      exit_port = p
    end
    opts.on("-d", "--debug_port [PORT]","Starts a pry-remote server on the selected port. WARNING: This can be a gaping security hole if your port access rules are not sane.") do |p|
      debug_port = p
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
      log_file = File.new(log_file, 'a')
      log_file.sync = true
      log = Logger.new(log_file)
      $stderr = log_file
    else
      if quiet
        log_file = RUBY_PLATFORM =~ /mswin|mingw/ ? 'NUL:' : '/dev/null'
      end
    end
    s_map = {'unknown'=>Logger::UNKNOWN,'fatal'=>Logger::FATAL,
      'error'=>Logger::ERROR,'warn'=>Logger::WARN,
      'info'=>Logger::INFO,'debug'=>Logger::DEBUG}

    log = DualLogger.new({:progname=>app_name,
      :ip=>EZQ.get_local_ip(),
      :filename=>log_file,
      :local_level=>s_map[severity],
      :loggly_token=>loggly_token,
      :loggly_level=>s_map[loggly_severity],
      :pid=>Process.pid})

    log.unknown "\n\n==============EZQ.Processor started================"

    if debug_port
      log.unknown "Pry-remote port: #{debug_port}"
      require 'pry-remote'
      Thread.new{loop{binding.remote_pry('localhost',debug_port.to_i)}}
    end

    if !File.exists?(creds_file)
      log.fatal "Credentials file '#{creds_file}' does not exist! Aborting."
      exit 1
    end

    credentials = YAML.load(File.read(creds_file))
    if !credentials.kind_of?(Hash)
      log.fatal "Credentials file '#{creds_file}' is not properly formatted! Aborting."
      exit 1
    end

    overrides = queue ? {"receive_queue_name"=>queue} : {}
    overrides['error_queue_name'] = error_queue if error_queue
    overrides['exit_port'] = exit_port
    EZQ::Processor.new(config_file,credentials,log,overrides).start
    # Handle Ctrl-C gracefully
  rescue Interrupt
    warn "\nEZQ.Processor aborted!"
    exit 1
  end
end
################################################################################
