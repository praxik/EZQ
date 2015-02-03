#!/usr/bin/env ruby

require 'bundler/setup'
require 'optparse'
require 'json'
require 'yaml'
require './dual_log'
require_relative './pzm_reportmaker'
require_relative './ezqlib'


quiet = false
creds_file = 'credentials.yml'
severity = 'info'
loggly_token = nil
loggly_severity = 'info'
app_name = 'pzm_report'
log_file = STDOUT
json_file = nil
job_id = nil

begin
  userdata = YAML.load_file('userdata.yml')
  loggly_token = userdata.fetch('loggly_token','')
  s3_bucket = userdata['report_bucket']
  s3_key_base = userdata['report_key_base']
rescue
end

op = OptionParser.new do |opts|
  opts.banner = "Usage: pzm_report.rb [options] JSON_FILE MSG_ID INPUT_FILE"
  # JSON_FILE is the input JSON containing scenarios, budgets, etc.
  # MSG_ID is the EZQ message id for this task. It's needed so we can
  # write out the correct filename for EZQ to send to result queue.
  # INPUT_FILE is the EZQ message, which contains other data needed to
  # correctly process the report.

  opts.on("-q", "--quiet", "Run quietly") do |q|
    quiet = q
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
  puts "pzm_reportmaker started.\n\n" unless quiet

  if quiet && log_file == STDOUT
    log_file = RUBY_PLATFORM =~ /mswin|mingw/ ? 'NUL:' : '/dev/null'
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

  if !File.exists?(creds_file)
    log.fatal "Credentials file '#{creds_file}' does not exist! Aborting."
    exit 1
  end

  credentials = YAML.load(File.read(creds_file))
  if !credentials.kind_of?(Hash)
    log.fatal "Credentials file '#{creds_file}' is not properly formatted! Aborting."
    exit 1
  end

  json_file = ARGV.shift

  msg_id = ARGV.shift

  input_file = ARGV.shift

  if !json_file
    @log.fatal "No input json file given. Aborting."
    exit(1)
  end

  if !msg_id
    @log.fatal "No message id given. Aborting."
    exit(1)
  end

  if !input_file
    @log.fatal "No input json element provided from the web app. Aborting."
    exit(1)
  end

  json = JSON.parse(File.read(json_file)).first
  eid = json.fetch('enterprise_id','000')
  fid = json.fetch('id','000')
  job_id = "#{eid}_#{fid}"

  # The incoming message is a single JSON object containing the key-value pair
  # "report_record_id"
  json_sqs = JSON.parse(File.read(input_file))
  report_id = json_sqs['report_record_id']

  report_fname = "#{job_id}_report.pdf"
  output_report = "report/#{report_fname}"
  PzmReportmaker.new(credentials,log).make_report(json_file,output_report)

  s3_key = "#{s3_key_base}/#{report_fname}"
  EZQ.send_file_to_s3(output_report, s3_bucket, s3_key)

  # Result message for EZQ::Processor to pick up
  result = {"worker_succeeded"=>true,
            "pdf_report"=>s3_key,
            "type"=>"report",
            "report_record_id"=>report_id}
  File.write("output_#{msg_id}.txt",result.to_json)

# Handle Ctrl-C gracefully
rescue Interrupt
  warn "\npzm_report aborted!"
  exit 1
end
