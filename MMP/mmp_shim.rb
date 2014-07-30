# This shim script is intended to sit between EZQ::Processor and the mmp_worker
# binary to handle bookeeping and other things that are not part of mmp_worker's
# core functionality.
#
# This script populates the settings required for correctly running mmp_worker,
# and then runs mmp_worker. If mmp_worker exits successfully, this script will
# also package up a "success!" message that EZQ will place in a result queue.
#
# This script expects 3 positional commandline arguments:
# 1. A JSON input file containing the key "report_record_id"
# 2. The pid of the calling process
# 3. The name of an outputfile to which it should write its "success!" message
#
# All of the above arguments are set in mmp_example_config.yml

require 'json'
require 'yaml'
require_relative 'ezqlib'
require 'logger'
require 'fileutils'

lf = File.new('mmp_shim.log', 'a')
lf.sync = true
log = Logger.new(lf)
log.level = Logger::WARN

log.info 'Setting up AWS'
AWS.config(YAML.load(File.read('credentials.yml')))

log.info 'Retrieving userdata'
vars = YAML.load(File.read('userdata.yml'))

db_ip = vars['db_ip']
db_port = vars['db_port']
db_user_name = vars['db_user_name']
db_password = vars['db_password']
db_name = vars['db_name']
geoserver_ip = vars['geoserver_ip']
geoserver_port = vars['geoserver_port']
s3_bucket = vars['s3_bucket']

input_file = ARGV.shift
pid = ARGV.shift # Caller's pid. Doug indicated this would be needed to separate
                 # multiple processes on the same instance. Not sure where it
                 # will go in the command string.
output_file = ARGV.shift
worker_id = pid

# The incoming message is a single JSON object containing the key-value pair
# "report_record_id"
report_record_id = JSON.parse(File.read(input_file))['report_record_id']
log.info "Operating on report_record_id: #{report_record_id}"

command = "mmp_worker.exe" +
          " -i #{worker_id}" +
          " --praxik-dev-postgres Server=#{db_ip};" +
                                 "Port=#{db_port};" +
                                 "Uid=#{db_user_name};" +
                                 "Pwd=#{db_password};" +
                                 "Database=#{db_name};" +
          " --praxik-gis-server Server=#{geoserver_ip};" +
                               "Port=#{geoserver_port};" +
                               "Uid=postgres;" +
                               "Pwd=postgres;" +
          " -j #{report_record_id}" +
          " -s #{s3_bucket}"

# Run the command we just set up, pushing its stdout and stderr back up the
# chain to the calling process. We also capture the command's exit status so
# this script can exit with the same status.
exit_status = 0
has_errors = false
errors = []
already_pushed = []
push_threads = []
begin
  IO.popen(command,:err=>[:child, :out]) do |io|
    while !io.eof?
      msg = io.gets
      log.info "Message: #{msg}"
      if msg =~ /^push_file/
        # Don't push the same file multiple times during a job.
        bucket_comma_filename = msg.sub!(/^push_file\s*:\s*/,'')
        log.info "Push_file directive for #{bucket_comma_filename}"
        if !already_pushed.include?(bucket_comma_filename)
          log.info "File has not been pushed previously. Doing so now...."
          begin
            #If we want to reference files via cwd
            #bucket,key = bucket_comma_filename.split(',').map{|s| s.strip}
            #fname = File.basename(key)
            #push_threads << EZQ.send_file_to_s3_async(fname,bucket,key)
            #If we want to reference a file to mirror what is in s3
            push_threads << EZQ.send_bcf_to_s3_async(bucket_comma_filename)
            already_pushed << bucket_comma_filename
          rescue => e
            log.error e
            puts e
          end
        end
      elsif msg =~ /^error_message/
        errors << msg.gsub(/^error_message/,'')
        has_errors = true
        puts msg
      else
        puts msg
      end
    end
    io.close
    exit_status = $?.to_i
  end
rescue => e
  exit_status = 1
  log.error e
  puts e
end

log.info 'Waiting for file push threads to finish.'
push_threads.each{|t| t.join()}

log.info 'Deleting local files pushed to S3.'
already_pushed.each do |bcf|
  bucket,key = bcf.split(',').map{|s| s.strip}
  #If we want to reference files via cwd
  #FileUtils.rm(File.basename(key))
  FileUtils.rm(key)
end

# The message written here will be picked up by EZQ::Processor and placed into
# the result queue specified in mmp_example_config.yml.
if exit_status.zero?
  result_message = {}
  result_message['report_record_id'] = report_record_id
  result_message['worker_succeeded'] = !has_errors
  if has_errors
    result_message['errors'] = errors.join("\n")
  end
  File.write(output_file,result_message.to_json)
end

log.info 'Done.'
exit(exit_status)
