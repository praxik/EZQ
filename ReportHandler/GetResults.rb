#!/use/bin/env ruby

# This application serves the role of ReportHandler for rusle2 data.
require 'bundler/setup'
require './processor.rb'
require 'json'
require 'aws-sdk'
require 'socket'
require './data_spec_parser.rb'

# This class overrides a few chosen methods of EZQ processor to insert task
# tracking into the flow.

class RusleReport < EZQ::Processor

  def initialize(configuration,credentials,logger = nil,overrides={})
    # The input file should be the only thing left in ARGV after options
    # processing.
    filename = ARGV[0]
    exit 1 if !File.exists?(filename)
    job_details = JSON.parse(File.read(filename))

    @rr_job_id = job_details['job_id']
    @rr_task_ids = job_details['task_ids']
    @gen_dom_crit_report = job_details.fetch('generate_dominant_critical_soil_report',false)
    @batch_mode = job_details.fetch('batch_mode',false)

    # Each time a task is completed, we remove it from rr_remaining_tasks.
    # When rr_remaining_tasks is empty, we know we've processed all tasks
    # related to this job.
    @rr_remaining_tasks = @rr_task_ids.clone
    @rr_completed_tasks = []
    @waiting_for_report = false

    overrides={"receive_queue_name"=>@rr_job_id}
    # Set up the parent Processor
    super
  end

  protected
  # Override of EZQ::Processor method to add logic to keep track of completed
  # tasks.
  def run_process_command(msg,input_filename,id,log_command=true)
    # Open up the results and insert the current job id
    parsed_msg = JSON.parse(@msg_contents)
    parsed_msg['job_id'] = @rr_job_id
    @record_id = parsed_msg['record_id'] if @gen_dom_crit_report
    @msg_contents = parsed_msg.to_json

    success = true
    task_id = parsed_msg['task_id']
    # Only process the task for real if we haven't already seen it; otherwise
    # just return true so the task will be deleted.
    if @waiting_for_report
      # Check to see if it's the report. If so, process it; if not, sink it.
      if @msg_contents =~ /manop_table/
        success = super
      else
        @logger.info "Sinking extra result message while waiting for report"
        success = true
      end
    else
      if !@rr_completed_tasks.include?(task_id)
        success = super
      else
        @logger.info "Sinking duplicate result message"
        success = true
      end
    end

    # If parent processing was successful, mark the task as completed
    if success
      @rr_remaining_tasks.delete(task_id)
      @rr_completed_tasks << task_id
      return true
    else
      return false
    end
  end



  protected
  # Override of EZQ::Processor method to add logic to exit if all tasks
  # associated with this job have been completed.
  def poll_queue
    multi_batch_poll() if @batch_mode
    @in_queue.poll_no_delete(@polling_options) do |msg|
      # Batch mode should have been set as a flag in the report_gen message
      # if we're to use it.
      if @batch_mode
        success = false
        success = process_message_batch( Array(msg) )
        if success and @rr_remaining_tasks.empty?
         AWS::SQS.new.queues.named("#{@rr_job_id}").delete
            exit(0)
        end
      else
        Array(msg).each do |item|
          success = false
          success = process_message(item)
          if success and @rr_remaining_tasks.empty?
            deal_with_report if @gen_dom_crit_report # Breaks out of recursion in
                  # addition to simply seeing whether this step must be performed.
                                                     
            AWS::SQS.new.queues.named("#{@rr_job_id}").delete
            exit(0)
          end
        end
      end
    end
    # Polling has timed out based on value of :idle_timeout
    if !@rr_remaining_tasks.empty?
      errm = <<-END
        Reporthandler on instance #{@instance_id} has timed out while
        awaiting results from job #{@rr_job_id}.
        #{@rr_remaining_tasks.size} result sets have not been received.

        Their IDs are: #{@rr_remaining_tasks}
        END
      send_error(errm)
    end
  end


  def multi_batch_poll
    while true
      msgs = []
      stamp = Time.now.to_i
      while ( (msgs.size < 1000) and ((Time.now.to_i - stamp) < 20) )
        msgs += Array( @in_queue.receive_messages(:limit => 10) )
      end
      success = false
      success = process_message_batch( msgs )
      if success and @rr_remaining_tasks.empty?
       AWS::SQS.new.queues.named("#{@rr_job_id}").delete
          exit(0)
      end
    end
  end

  # Batch-mode messages are handled differently from single messages.
  # * file refs (S3 and uris) are not supported
  # * file_as_body is not supported
  # * decompression is not supported
  # The essential feature of batch mode processing is that it will retrieve
  # messages from a queue as quickly as possible, and hand them off to a
  # processor as quickly as possible. The whole thing is much less flexible than
  # single mode processing, but the benefit is an increase in throughput.
  # Generally speaking, the dont_hit_disk flag should be set to true when using
  # batch mode.
  def process_message_batch(msg_ary)
    @logger.unknown '------------------------------------------'
    #msg_ids = []
    #msg_ary.each{|msg| msg_ids << msg.id}
    @logger.info "Processing message batch of size #{msg_ary.size}"

    # We need *some* value for these two, and the id of the first message of the
    # batch works as well as anything else....
    @input_filename = msg_ary[0].id + '.in'
    @id = msg_ary[0].id

    task_ids = []

    msg_batch = {}
    msg_batch['batch_mode'] = true
    msg_ary.each_with_index do |msg,idx|
      strip_directives(msg)
      contents = JSON.parse(msg.body)
      task_ids << contents['task_id']
      contents['job_id'] = @rr_job_id
      msg.body.gsub!(/.+/,contents.to_json)
      msg_batch["record_#{idx}"] = contents
    end

    # Message "body" is a JSON string containing all the supplied message bodies
    body =  msg_batch.to_json
    
    @msg_contents = body
    File.open( "#{@input_filename}", 'w' ) { |output| output << body } unless @dont_hit_disk

    # The first argument is spurious, but run_process_command wasn't set up to
    # handle a batch. The argument is irrelevant to our needs here since it only
    # controls what gets written in store_message, and we don't use that info
    # for these batch mode aggregations anyway.
    #success = EZQ::Processor.instance_method(:run_process_command).bind(self).call(msg_ary[0],@input_filename,@id,false)
    success = socket_process_command(@msg_contents)
    
    if success
      @rr_remaining_tasks -= task_ids
      @rr_completed_tasks += task_ids
      # Do result_step before deleting the message in case result_step fails.
      if do_result_step()
        @logger.info "Processing successful. Deleting message batch of size #{msg_ary.size}"
        msg_ary.each_slice(10){|ary| @in_queue.batch_delete(*ary)}
      end
    end
    
    # Cleanup even if processing otherwise failed.
    cleanup(@input_filename,@id)
    return success
  end


  def socket_process_command(data)
    socket_path = '/tmp/rusle2'
    client_conn = UNIXSocket.new(socket_path)
    client_conn.puts(data) 
    client_conn.close_write()
    exit_code = client_conn.read()
    client_conn.close()
    if exit_code.to_i == 0
      @logger.info 'socket_process_command succeeded'
      return true
    else
      @logger.info 'socket_process_command failed with exit code #{exit_code}'
      return false
    end
  rescue => e
    @logger.error "socket_process_command failed with exception: #{e}"
    send_error("socket_process_command failed with exception: #{e}")
    return false
  end


  # TODO: clean this mess up. Break out functionality into smaller, digestible
  # pieces.
  def deal_with_report
    @logger.info 'deal_with_report'
    data_spec = DataSpecParser::get_data_spec('main.cxx')
    tablename = data_spec[:tablename]
    # 6k_aggregator c++ app will look at the soil geojson and tell us which
    # is the dominant critical soil
    dom_crit_id = 0
    cpp_output = ''
    #LD_LIBRARY_PATH=. ./6k_aggregator -j c386223a-1838-4bc8-b39d-307b37e759af -f c386223a-1838-4bc8-b39d-307b37e759af_job.json -t isa2_results -d "Driver=PostgreSQL;Server=development-rds-pgsq.csr7bxits1yb.us-east-1.rds.amazonaws.com;Port=5432;Uid=app;Pwd=app;Database=praxik;" --ssurgoconnstr "Driver=PostgreSQL;Server=10.1.2.8;Port=5432;Uid=postgres;Pwd=postgres;Database=ssurgo;" --connector ODBC
    command = "LD_LIBRARY_PATH=. ./6k_aggregator -j #{@rr_job_id} -r #{@record_id} -f json/#{@rr_job_id}_#{@record_id}_job.json -t #{tablename} -d \"Driver=PostgreSQL Unicode;Server=development-rds-pgsq.csr7bxits1yb.us-east-1.rds.amazonaws.com;Port=5432;Uid=app;Pwd=app;Database=praxik;\" --ssurgoconnstr \"Driver=PostgreSQL Unicode;Server=10.1.2.8;Port=5432;Uid=postgres;Pwd=postgres;Database=ssurgo;\" --connector ODBC"
    @logger.info "\n\n#{command}\n\n"
    IO.popen(command) do |io|
      while !io.eof?
        cpp_output = io.gets
      end
    end
    Dir.mkdir('report_data') unless Dir.exists?('report_data')
    File.write("report_data/#{@rr_job_id}_#{@record_id}.geojson",JSON.parse(cpp_output)['soil_geojson'].to_json)
    File.write("report_data/#{@rr_job_id}_#{@record_id}.json",cpp_output)
    dom_crit_id = JSON.parse(cpp_output)['task_id']
    @logger.info "dom_crit_id is #{dom_crit_id}"
    # Search through input_data.json to get the input structure for task_id
    # dom_crit_id.
    inputs = File.read("report_data/#{@rr_job_id}_input_data.json").split('####')
    i_data = {}
    inputs.each do |input|
      i_data = JSON.parse(input)
      break if i_data['task_id'] == dom_crit_id
    end
    @logger.info "Found task_id in report_data/#{@rr_job_id}_input_data.json." unless i_data.empty?
    # Add kv pair report : true to the top level of this structure,
    i_data['report'] = true
    # Form up an EZQ header for get_s3_files.
    # We rely on the fact that we know exactly how the the values of the soiR2
    # and ifcWeps keys map to filenames. A better solution would be to put a
    # Ruby wrapper around the worker process that pulls in and escapes the
    # full message, with header, and appends it to the result output as a new
    # field. Then deal_with_report doesn't have to know all the gory details
    # of the non-report worker result messages.
    soiR2 = "share/leaf/rusle2/#{i_data['soiR2']}"
    ifcWeps = "share/leaf/weps/weps.wjr/#{i_data['ifcWeps']}"
    preamble = {}
    ezq = {}
    preamble['EZQ'] = ezq
    ezq['result_queue_name'] = @rr_job_id
    pushed_files = []
    pushed_files.push(Hash['bucket'=>'6k_test.praxik','key'=>soiR2])
    pushed_files.push(Hash['bucket'=>'6k_test.praxik','key'=>ifcWeps])
    ezq['get_s3_files'] = pushed_files
    preamble = preamble.to_yaml
    preamble += "...\n"

    msg = "#{preamble}#{i_data.to_json}"

    @logger.info "Sending this message back for pass 2: #{msg}"
    
    # Place task back into worker task queue.
    # FIXME: hardcoding the queue name like this will break spot instance
    # workflows.
    task_q_name = '6k_task_test_44'
    queue = AWS::SQS.new.queues.named(task_q_name)
    
    if !queue.exists?
      @logger.fatal "No queue named #{task_q_name} exists."
      raise "No queue named #{task_q_name} exists."
    end
    queue.send_message(msg)

    # Start polling again.
    @waiting_for_report = true
    @dont_hit_disk = false # We want the results to be written this time!
    @process_command = "ruby pdf_report.rb $input_file #{@rr_job_id}"
    @gen_dom_crit_report = false # This will trigger a graceful exit after
                                 # pdf_report finishes.
    start
  end

end

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
  log_file = STDOUT
  op = OptionParser.new do |opts|
    opts.banner = "Usage: rusle2_report.rb [options]"

    opts.on("-q", "--quiet", "Run quietly") do |q|
      quiet = q
    end
    opts.on("-c", "--config [CONFIG_FILE]", "Use configuration file CONFIG_FILE. Defaults to queue_config.yml if not specified.") do |file|
      config_file = file
    end
    opts.on("-l", "--log [LOG_FILE]","Log to file LOG_FILE") do |file|
      log_file = file
    end
    opts.on("-s", "--log_severity [SEVERITY]","Set log severity to one of: unknown, fatal, error, warn, info, debug. Default = info") do |s|
      severity = s
    end
    opts.on("-r", "--credentials [CREDS_FILE]","Use credentials file CREDS_FILE. Defaults to credentials.yml if not specified.") do |file|
      creds_file = file
    end
    opts.on("-Q", "--queue [QUEUE_NAME]","Poll QUEUE_NAME for tasks rather than the receive_queue specified in the config file") do |q|
      queue = q
    end
  end

  begin op.parse!
  rescue OptionParser::InvalidOption => e
    if !quiet
      puts e
      puts op
    end
    exit 1
  end

  begin
    puts "rusle2_report started.\n\n" unless quiet
    if log_file != STDOUT
      lf = File.new(log_file, 'a')
      lf.sync = true
      log = Logger.new(lf)
      $stderr = lf
    else
      if !quiet
        log = Logger.new(STDOUT)
      else
        log_file = RUBY_PLATFORM =~ /mswin|mingw/ ? 'NUL:' : '/dev/null'
        log = Logger.new(log_file)
      end
    end
    s_map = {'unknown'=>Logger::UNKNOWN,'fatal'=>Logger::FATAL,
             'error'=>Logger::ERROR,'warn'=>Logger::WARN,
             'info'=>Logger::INFO,'debug'=>Logger::DEBUG}
    log.level = s_map[severity]
    

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
    RusleReport.new(config_file,credentials,log,overrides).start
  # Handle Ctrl-C gracefully
  rescue Interrupt
    warn "\nrusle2_report aborted!"
    exit 1
  end
end
################################################################################



