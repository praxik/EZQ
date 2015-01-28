#!/usr/bin/env ruby

require 'bundler/setup'
require 'optparse'
require 'json'
require 'yaml'
require './dual_log'
require_relative './landsat_worker'
require_relative './ezqlib'

s3_bucket = 'landsat.agsolver'

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
rescue
end

op = OptionParser.new do |opts|
  opts.banner = "Usage: landsat_app.rb [options] GEOJSON_FILE MSG_ID INPUT_FILE"
  # GEOJSON_FILE is the input geojson containing the AOI
  # MSG_ID is the EZQ message id for this task. It's needed so we can
  # write out the correct filename for EZQ to send to result queue.
  # INPUT_FILE is the EZQ message, which contains the landsat sceneID to be used

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
  puts "landsat_app started.\n\n" unless quiet

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

  aoi_file = ARGV.shift

  msg_id = ARGV.shift

  input_file = ARGV.shift

  if !aoi_file
    @log.fatal "No input geojson file given. Aborting."
    exit(1)
  end

  if !msg_id
    @log.fatal "No message id given. Aborting."
    exit(1)
  end

  if !input_file
    @log.fatal "No message file provided. Aborting."
    exit(1)
  end

  AWS.config(credentials)

  j = JSON.parse(File.read(input_file))
  scene_id = j.fetch('scene_id',nil)
  job_id = j.fetch('job_id','default_job')

  if !scene_id
    @log.fatal "No scene id in message!"
    exit(1)
  end

  LandsatWorker.new.process_scene(scene_id,aoi_file)

  # Push output images into S3 and delete local copies.
  images = ['ndvi_3857.tif','ndvi_4326.tif','yld_3857.tif']
  images.each do |img|
    file = "#{scene_id}/#{img}"
    s3_key = "ndvi/#{job_id}/#{img}"
    EZQ.send_file_to_s3(file, s3_bucket, s3_key)
    File.unlink(file) if File.exist?(file)
  end

  # Result message for EZQ::Processor to pick up
  result = {"worker_succeeded"=>true,
            "job_id"=>job_id,
            "ndvi_bucket"=>s3_bucket,
            "ndvis"=>images.map{|img| "ndvi/#{job_id}/#{img}"} }
  File.write("output_#{msg_id}.txt",result.to_json)

# Handle Ctrl-C gracefully
rescue Interrupt
  warn "\npzm_report aborted!"
  exit 1
end



# Penn, don't forget to invoke this as
# ruby -I . script_name.rb
# to account for the load_path needed for the leaf wrappers
