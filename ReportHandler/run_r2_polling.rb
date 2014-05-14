#!/use/bin/env ruby

# This application serves the role of ReportHandler for rusle2 data.
require 'bundler/setup'
require './processor.rb'
require 'json'
require 'aws-sdk'
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
    @gen_dom_crit_report = job_details['generate_dominant_critical_soil_report']

    # Each time a task is completed, we remove it from rr_remaining_tasks.
    # When rr_remaining_tasks is empty, we know we've processed all tasks
    # related to this job.
    @rr_remaining_tasks = @rr_task_ids.clone

    overrides={"receive_queue_name"=>@rr_job_id}
    # Set up the parent Processor
    super
  end

  protected
  # Override of EZQ::Processor method to add logic to keep track of completed
  # tasks.
  def run_process_command(msg,input_filename,id)
    # Open up the results and insert the current job id
    parsed_msg = JSON.parse(@msg_contents)
    parsed_msg['job_id'] = @rr_job_id
    @msg_contents = parsed_msg.to_json
    
    success = super

    # If parent processing was successful, mark the task as completed
    if success
      task_id = JSON.parse(@msg_contents)['task_id']
      @rr_remaining_tasks.delete(task_id)
      return true
    else
      return false
    end
  end

  protected
  # Override of EZQ::Processor method to add logic to exit if all tasks
  # associated with this job have been completed.
  def poll_queue
    @in_queue.poll_no_delete(@polling_options) do |msg|
      Array(msg).each do |item|
        process_message(item)
        if @rr_remaining_tasks.empty?
          deal_with_report if @gen_dom_crit_report # Breaks out of recursion in
                # addition to simply seeing whether this step must be performed.
                                                   
          AWS::SQS.new.queues.named("#{@rr_job_id}").delete
          exit(0)
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
    command = "LD_LIBRARY_PATH=. ./6k_aggregator -j #{@rr_job_id} -f json/#{@rr_job_id}_#{@record_id}_job.json -t #{tablename} -d \"Driver=PostgreSQL;Server=development-rds-pgsq.csr7bxits1yb.us-east-1.rds.amazonaws.com;Port=5432;Uid=app;Pwd=app;Database=praxik;\" --ssurgoconnstr \"Driver=PostgreSQL;Server=10.1.2.8;Port=5432;Uid=postgres;Pwd=postgres;Database=ssurgo;\" --connector ODBC"
    @logger.info "\n\n#{command}\n\n"
    IO.popen(command) do |io|
      while !io.eof?
        cpp_output = io.gets
      end
    end
    Dir.mkdir('report_data') unless Dir.exists?('report_data')
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
    @logger.info "Found task_id in input_data.json." unless i_data.empty?
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
    opts.on("-r", "--credentials [CREDS_FILE]","Use credentials file CREDS_FILE. Defaults to credentials.yml if not specified.") do |file|
      creds_file = file
    end
    opts.on("-q", "--queue [QUEUE_NAME]","Poll QUEUE_NAME for tasks rather than the receive_queue specified in the config file") do |q|
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
    RusleReport.new(config_file,credentials,log,overrides).start
  # Handle Ctrl-C gracefully
  rescue Interrupt
    warn "\nrusle2_report aborted!"
    exit 1
  end
end
################################################################################



