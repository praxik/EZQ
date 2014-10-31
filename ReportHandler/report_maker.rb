#!/use/bin/env ruby

# This application serves the role of ReportHandler for rusle2 data.
require 'bundler/setup'
require './processor.rb'
require 'json'
require 'aws-sdk'
require 'fileutils'


class ReportMaker < EZQ::Processor

  def initialize(configuration,credentials,logger = nil,overrides={})

    @credentials = credentials

    # The input file should be the only thing left in ARGV after options
    # processing.
    filename = ARGV[0]
    exit 1 if !File.exists?(filename)
    @settings = JSON.parse(File.read(filename))
    # The queue is named as jobid_dom_crit_results
    @job_id = @settings['queue_to_poll'].split('_')[0]

    #overrides={"receive_queue_name"=>@job_id}
    overrides={"receive_queue_name"=>@settings['queue_to_poll'],
               "process_command"=>"ruby pdf_report.rb $input_file #{@job_id}",
               "atomicity"=>@settings["atomicity"]}
    # Set up the parent Processor
    super
  end


  #protected
   ##Override process_message so this behaves as a single-shot processor.
  #def process_message(msg)
    #success = super
    #exit(0) if success
  #end

  # This override is used when atomicity > 1
  def process_molecule(mol)
    success = super
    if success
      AWS::SQS.new.queues.named(@settings['queue_to_poll']).delete
      exit(0)
    else
      @logger.error("Process_molecule error")
      exit(1)
    end
  end

  # This override is used when atomicity == 1
  def process_message(msg)
    success = super
    if success
      AWS::SQS.new.queues.named(@settings['queue_to_poll']).delete
      exit(0)
    else
      @logger.error("Process_message error")
      exit(1)
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
  severity = 'info'
  queue = nil
  log_file = STDOUT
  op = OptionParser.new do |opts|
    opts.banner = "Usage: report_maker.rb [options]"

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
    puts "report_maker started.\n\n" unless quiet
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
    ReportMaker.new(config_file,credentials,log,overrides).start
  # Handle Ctrl-C gracefully
  rescue Interrupt
    warn "\nreport_maker aborted!"
    exit 1
  end
end
################################################################################



