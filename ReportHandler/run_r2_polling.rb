#!/use/bin/env ruby

# This application serves the role of ReportHandler for rusle2 data.
require 'bundler/setup'
require './processor.rb'
require 'json'
require 'aws-sdk'


# What I want to do here is to call EZQ::processor, giving it job_id as the
# receive_queue_name, and have it in turn call rusle2_aggregator.rb with
# each result message. The problem is that this program has no way of knowing
# when a given message has been pulled down and stored in the db. It leaves no
# way to remove tasks from remaining_tasks.

# One option is to copy over the queue polling code from EZQ::processor and
# manually do that work here instead of starting up an EZQ::processor.

# A somewhat better strategy would be to inherit from EZQ::processor and
# override run_process_command, storing the return value of super in a
# boolean. If true, then here I can look at the msg contents, identify the
# task id, and remove it from my list.

# That last one seems like a better strategy than messing with EZQ::processor
# to hack some sort socket or stdout communication into it, which would be
# another option.

class RusleReport < EZQ::Processor

  def initialize(configuration,credentials,logger = nil,overrides={})
    # The input file should be the only thing left in ARGV after options
    # processing.
    filename = ARGV[0]
    exit 1 if !File.exists?(filename)
    job_details = JSON.parse(File.read(filename))

    @rr_job_id = job_details['job_id']
    @rr_task_ids = job_details['task_ids']

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
    parsed_msg = JSON.parse(msg)
    parsed_msg['job_id'] = @rr_job_id
    msg = parsed_msg.to_json
    
    success = super

    # If parent processing was successful, mark the task as completed
    if success
      task_id = JSON.parse(msg)['task_id']
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
          AWS::SQS.new.queues.named("#{@job_id}").delete
          exit(0)
        end
      end
    end
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
      log = Logger.new(lf)
      $stderr = lf
    else
      log = Logger.new(log_file)
    end
    if quiet && log_file == STDOUT
      log.level = Logger::UNKNOWN
    else
      log.level = Logger::DEBUG
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



