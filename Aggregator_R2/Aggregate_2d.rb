#!/use/bin/env ruby

# This application serves the role of ReportHandler for rusle2 data.
require 'bundler/setup'
require './processor.rb'
require 'json'
require 'aws-sdk'
require 'fileutils'
require 'deep_merge'
#require './data_spec_parser.rb'
require './ezqlib.rb'


# This class overrides a few chosen methods of EZQ processor to insert task
# tracking into the flow.

class Agg2d < EZQ::Processor

  def initialize(configuration,credentials,logger = nil,overrides={})
    # The input file should be the only thing left in ARGV after options
    # processing.
    filename = ARGV[0]
    exit 1 if !File.exists?(filename)
    @agg_settings = JSON.parse(File.read(filename))
    @credentials = credentials
    @job_id = @agg_settings['job_id']
    atomicity = @agg_settings['task_ids'].count
    @connection_string = @agg_settings.fetch('connection_string','')
    overrides={"receive_queue_name"=>@agg_settings['queue_to_aggregate'],"atomicity"=>atomicity}
    # Set up the parent Processor
    super
  end


  protected
  # override molecule processing so we can exit after one shot
  def process_molecule(mol)
    success = super
    if success
      send_results()
      AWS::SQS.new.queues.named(@agg_settings['queue_to_aggregate']).delete
      exit(0)
    else
      @logger.error("Process_molecule error")
      exit(1)
    end
  end


  protected
  # Override this to change the default value of log_command to false and to
  # override the process_command with the specified post-process command.
  def run_process_command(input_filename,id,log_command=false)
    merged_filename = split_and_merge(@input_filename)
    command = @agg_settings.fetch('post_process','')
    return false if command.empty?
    command.gsub!('$jobid',@job_id)
    command.gsub!('$recordid',@record_id)
    command.gsub!('$input_file',merged_filename)
    @process_command = command
    success = super
    FileUtils.rm(merged_filename)
    return success
  end


  # Run the post_process command
  #def run_post_process()
    #merged_filename = split_and_merge(@input_filename)
    #command = @agg_settings.fetch('post_process','')
    #return '' if command.empty?
    #command.gsub!('$jobid',@job_id)
    #command.gsub!('$recordid',@record_id)
    #command.gsub!('$input_file',merged_filename)
    #@logger.info "\n\n#{command}\n\n"
    #success = system(command)
    #FileUtils.rm(merged_filename)
    #return success
  #end

  # This is where we need to take the input file (which contains a concatenated
  # version of all the messages in the molecule), split it back up into separate
  # messages, do a deep merge on the contents, then write the results back out.
  def split_and_merge(file)
    msgs = File.read(file).split("#####!@$$@!#####")
    result = {}
    msgs.each{|f| result.deep_merge!(JSON.parse(EZQ.fix_escapes(f)))}
    @record_id = JSON.parse(EZQ.fix_escapes(msgs[0]))['record_id']
    fname = "#{file}.merged"
    File.write(fname,result.to_json)
    return fname
  end


  def send_results()
    prefix = "#{@job_id}_#{@record_id}"
    pics = ["#{prefix}_seg_soil_loss.tif","#{prefix}_seg_sed_load.tif"]
    bucket = '6k_test.praxik'
    keys = ["json/#{pics[0]}","json/#{pics[1]}"]
    EZQ.send_file_to_s3(pics[0],bucket,keys[0])
    EZQ.send_file_to_s3(pics[1],bucket,keys[1])

    preamble = {}
    ezq = {}
    preamble['EZQ'] = ezq
    pushed_files = []
    pushed_files << Hash['bucket'=>bucket,'key'=>keys[0]]
    pushed_files << Hash['bucket'=>bucket,'key'=>keys[1]]
    ezq['get_s3_files'] = pushed_files

    EZQ.enqueue_message("",preamble,"#{@job_id}_dom_crit_results",
                        false,'EZQOverflow.praxik')
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
    opts.banner = "Usage: Agg2d.rb [options]"

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
    puts "Agg2d started.\n\n" unless quiet
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
    Agg2d.new(config_file,credentials,log,overrides).start
  # Handle Ctrl-C gracefully
  rescue Interrupt
    warn "\nAgg2d aborted!"
    exit 1
  end
end
################################################################################



