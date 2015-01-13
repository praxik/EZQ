#!/usr/bin/env ruby

require 'bundler/setup'
require 'optparse'
require './dual_log'
require_relative './pzm_reportmaker'

quiet = false
creds_file = 'credentials.yml'
severity = 'info'
loggly_token = nil
loggly_severity = 'info'
app_name = 'pzm_report'
log_file = STDOUT
json_file = nil
job_id = nil

op = OptionParser.new do |opts|
  opts.banner = "Usage: pzm_report.rb [options] JSON_FILE JOB_ID"

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
  job_id = ARGV.shift

  if !json_file
    @log.fatal "No input json file given. Aborting."
    exit(1)
  end

  if !job_id
    @log.fatal "No job_id given. Aborting."
    exit(1)
  end

  output_report = "report/#{job_id}_report.pdf"
  PzmReportmaker.new(credentials,log).make_report(json_file,output_report)

# Handle Ctrl-C gracefully
rescue Interrupt
  warn "\npzm_report aborted!"
  exit 1
end
