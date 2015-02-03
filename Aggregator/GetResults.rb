#!/use/bin/env ruby

# This application serves the role of ReportHandler for rusle2 data.
require 'bundler/setup'
require './processor.rb'
require 'json'
require 'aws-sdk'
require 'socket'
require 'fileutils'
require 'pg'
require 'yaml'
require './data_spec_parser.rb'
require_relative './ezqlib'


# This class overrides a few chosen methods of EZQ processor to insert task
# tracking into the flow.

class RusleReport < EZQ::Processor

  def initialize(configuration,credentials,logger = nil,overrides={})
    # The input file should be the only thing left in ARGV after options
    # processing.
    filename = ARGV[0]
    exit 1 if !File.exists?(filename)
    @agg_settings = JSON.parse(File.read(filename))
    
    userdata = YAML.load_file('userdata.yml')
    @db_user_name = userdata['db_user_name']
    @db_password = userdata['db_password']
    @db_name = userdata['db_name']
    @db_ip = userdata['db_ip']
    @db_port = userdata['db_port']

    @credentials = credentials
    @job_id = @agg_settings['job_id']
    @task_ids = @agg_settings['task_ids']
    @gen_dom_crit_report = @agg_settings.fetch('generate_dominant_critical_soil_report',false)
    @batch_mode = @agg_settings.fetch('batch_mode',false)
    job_statistics = @agg_settings.fetch('job_statistics','')
    @store_inputs = @agg_settings.fetch('store_inputs',false)
    @tablename = @agg_settings['db_table']
    set_backend_table(@tablename)

    store_job_stats(job_statistics,@job_id) if !job_statistics.empty?

    # Each time a task is completed, we remove it from rr_remaining_tasks.
    # When rr_remaining_tasks is empty, we know we've processed all tasks
    # related to this job.
    @remaining_tasks = @task_ids.clone
    @completed_tasks = []

    @connection_string = @agg_settings.fetch('connection_string','')
    @report_files = @agg_settings.fetch('files_needed_to_make_report',nil)

    #overrides={"receive_queue_name"=>@job_id}
    overrides={"receive_queue_name"=>@agg_settings['queue_to_aggregate']}
    # Set up the parent Processor
    super
  end

  protected
  # Override of EZQ::Processor method to add logic to keep track of completed
  # tasks.
  def run_process_command(input_filename,id,log_command=true)
    # Open up the results and insert the current job id
    parsed_msg = JSON.parse(@msg_contents)
    parsed_msg['job_id'] = @job_id
    parsed_msg['aggregator_store_inputs'] = @store_inputs
    @record_id = parsed_msg['record_id'] if @gen_dom_crit_report
    @msg_contents = parsed_msg.to_json

    success = true
    task_id = parsed_msg['task_id']
    # Only process the task for real if we haven't already seen it; otherwise
    # just return true so the task will be deleted.
    if !@completed_tasks.include?(task_id)
      success = super
    else
      @logger.info "Sinking duplicate result message"
      success = true
    end

    # If parent processing was successful, mark the task as completed
    if success
      @remaining_tasks.delete(task_id)
      @completed_tasks << task_id
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
      Array(msg).each do |item|
        success = false
        success = process_message(item)
        if success and @remaining_tasks.empty?
          deal_with_report if @gen_dom_crit_report
          AWS::SQS.new.queues.named(@agg_settings['queue_to_aggregate']).delete
          exit(0)
        end
      end
    end
    # Polling has timed out based on value of :idle_timeout
    if !@remaining_tasks.empty?
      errm = <<-END
        Reporthandler on instance #{@instance_id} has timed out while
        awaiting results from job #{@job_id}.
        #{@remaining_tasks.size} result sets have not been received.

        Their IDs are: #{@remaining_tasks}
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
      next if msgs.empty? #continue polling if we came up completely empty
      success = false
      success = process_message_batch( msgs )
      if success and @remaining_tasks.empty?
       AWS::SQS.new.queues.named(@agg_settings['queue_to_aggregate']).delete
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
    @logger.info "Processing message batch of size #{msg_ary.size}"

    # We need *some* value for these two, and the id of the first message of the
    # batch works as well as anything else....
    @input_filename = msg_ary[0].id + '.in'
    @id = msg_ary[0].id

    task_ids = []
    msg_batch = hash_messages_and_yield_taskids(msg_ary){|id| task_ids << id}
    msg_batch['batch_mode'] = true

    # Message "body" is a JSON string containing all the supplied message bodies
    body =  msg_batch.to_json
    
    @msg_contents = body
    File.open( "#{@input_filename}", 'w' ) { |output| output << body } unless @dont_hit_disk

    success = socket_process_command(@msg_contents)
    
    if success
      @remaining_tasks -= task_ids
      @completed_tasks += task_ids
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

  # Returns a hash containing each message's contents associated with a unique
  # key. At the same time, this inserts the job_id into the message, and yields
  # the task id for each message. It's goofy, but combining these
  # operations like this allows us to avoid the expensive operation of parsing
  # the messages as json multiple times. Speed is important for batch processing.
  def hash_messages_and_yield_taskids(ary)
    task_ids = []
    msg_batch = {}
    ary.each_with_index do |msg,idx|
      begin
        strip_directives(msg)
        contents = JSON.parse(msg.body)
        yield contents['task_id']
        contents['job_id'] = @job_id
        msg.body.gsub!(/.+/,contents.to_json)
        msg_batch["record_#{idx}"] = contents
      rescue => e
        @logger.error "Error parsing JSON, skipping result: #{msg.body}"
      end
    end
    return msg_batch
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



  def set_backend_table(tablename)
    socket_process_command({'set_table'=>tablename}.to_json())
  end


  # TODO: clean this mess up. Break out functionality into smaller, digestible
  # pieces.
  def deal_with_report
    @logger.info 'deal_with_report'
    data_spec = DataSpecParser::get_data_spec('main.cxx')
    #tablename = @agg_settings['db_table']
    dom_crit_id = 0
    
    cpp_output = run_post_process(@tablename)
    
    Dir.mkdir('report_data') unless Dir.exists?('report_data')
    geojsonfile = "report_data/#{@job_id}_#{@record_id}.geojson"
    jsonfile = "report_data/#{@job_id}_#{@record_id}.json"

    write_and_push(geojsonfile,JSON.parse(cpp_output)['soil_geojson'].to_json)
    write_and_push(jsonfile,cpp_output)
    
    dom_crit_id = JSON.parse(cpp_output)['task_id']
    @logger.info "dom_crit_id is #{dom_crit_id}"

    i_data = {}
    i_data = get_inputs(dom_crit_id)

    # Add kv pair report : true to the top level of this structure. Worker looks
    # for this kv pair to determine if it should dump out the special report
    # results.
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
    ezq['result_queue_name'] = "#{@job_id}_dom_crit_results"
    pushed_files = []
    pushed_files.push(Hash['bucket'=>'6k_test.praxik','key'=>soiR2])
    pushed_files.push(Hash['bucket'=>'6k_test.praxik','key'=>ifcWeps])
    ezq['get_s3_files'] = pushed_files
    preamble = preamble.to_yaml
    preamble += "...\n"

    msg = "#{preamble}#{i_data.to_json}"

    @logger.info "Sending this message back for pass 2: #{msg}"
    
    # Place task back into worker task queue.
    task_q_name = @agg_settings['worker_task_queue']
    queue = AWS::SQS.new.queues.named(task_q_name)
    
    if !queue.exists?
      @logger.fatal "No queue named #{task_q_name} exists."
      send_error("No queue named #{task_q_name} exists.",true)
    end
    queue.send_message(msg)

    # At this point we should be able to safely drop the temporary db table
    # containing the worker inputs
    @db.exec("drop table _#{@job_id.gsub('-','')}_inputs")

    # Clean up the files we've successfully sent to S3
    FileUtils.rm(geojsonfile)
    FileUtils.rm(jsonfile)

    # Form up message to send to report gen stage
    preamble = {}
    ezq = {}
    preamble['EZQ'] = ezq
    pushed_files = []
    pushed_files << Hash['bucket'=>'6k_test.praxik','key'=>geojsonfile]
    pushed_files << Hash['bucket'=>'6k_test.praxik','key'=>jsonfile]
    pushed_files += (@report_files)
    ezq['get_s3_files'] = pushed_files
    preamble = preamble.to_yaml
    preamble += "...\n"
    msgdata = {}
    dom_crit_results_queue = "#{@job_id}_dom_crit_results"
    msgdata['queue_to_poll'] = dom_crit_results_queue
    msgdata['atomicity'] = 2
    msg = "#{preamble}#{msgdata.to_json}"

    # Create the dom crit results queue for this job
    #sqs = AWS::SQS.new
    #begin
      #q = sqs.queues.named( dom_crit_results_queue )
    #rescue
      #sqs.queues.create( dom_crit_results_queue )
    #end

    # Send the report gen message out
    rgq = '6k_report_gen'
    queue = AWS::SQS.new.queues.named(rgq)
    
    if !queue.exists?
      @logger.fatal "No queue named #{rgq} exists."
      raise "No queue named #{rgq} exists."
    end
    queue.send_message(msg)
  end


  # Run the post_process command
  def run_post_process(tablename)
    #command = "LD_LIBRARY_PATH=. ./6k_aggregator -j #{@job_id} -r #{@record_id} -f json/#{@job_id}_#{@record_id}_job.json -t #{tablename} -d \"Driver=PostgreSQL Unicode;Server=development-rds-pgsq.csr7bxits1yb.us-east-1.rds.amazonaws.com;Port=5432;Uid=app;Pwd=app;Database=praxik;\" --ssurgoconnstr \"Driver=PostgreSQL Unicode;Server=10.1.2.8;Port=5432;Uid=postgres;Pwd=postgres;Database=ssurgo;\" --connector ODBC"
    command = @agg_settings.fetch('post_process','')
    return '' if command.empty?
    command.gsub!('$jobid',@job_id)
    command.gsub!('$recordid',@record_id)
    command.gsub!('$tablename',tablename)
    @logger.info "\n\n#{command}\n\n"
    output = ''
    IO.popen(command) do |io|
      while !io.eof?
        output = io.gets
      end
    end
    @logger.info "output: #{output}"
    return(output)
  end


  # Write a file to disk and immediately send it out to S3
  def write_and_push(name,content)
    File.write(name,content)
    EZQ.send_file_to_s3(name,"6k_test.praxik",name)
  end


  # Hit the db to get inputs associated with the dom_crit task
  def get_inputs(dom_crit_id)
    @db = PG.connect(
        host: @db_ip,
        dbname: @db_name,
        user: @db_user_name,
        password: @db_password)
    sql = "select inputs from _#{@job_id.gsub('-','')}_inputs where task_id=#{dom_crit_id}"
    result = @db.exec(sql)
    @logger.info "result: #{result[0]}"
    return(JSON.parse(result[0]['inputs']))
  end


  def store_job_stats(stats,job_id)
    db = PG.connect(
        host: @db_ip,
        dbname: @db_name,
        user: @db_user_name,
        password: @db_password)
    tablename = 'job_statistics_6k'
    sql = "create table if not exists #{tablename}( job_id text, stats text )"
    db.exec( sql )
    result = db.exec_params(%[ INSERT INTO #{tablename} (job_id, stats) VALUES($1, $2) ],
            [job_id,EZQ.unescape(stats)])
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


