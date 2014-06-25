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

# Do whatever is required to fill out these vars (Something set in environment?
# Query the instance's userdata? Read in a file that was written at boot?)
worker_id = ''
db_ip = ''
port = ''
db_user_name = ''
db_passoword = ''
db_name = ''
geoserver_ip = ''
db_port = ''

input_file = ARGV.shift
pid = ARGV.shift # Caller's pid. Doug indicated this would be needed to separate
                 # multiple processes on the same instance. Not sure where it
                 # will go in the command string.
output_file = ARGV.shift

# The incoming message is a single JSON object containing the key-value pair
# "report_record_id"
report_record_id = JSON.parse(File.read(input_file))['report_record_id']

command = "mmp_worker" +
          " -i #{worker_id}" +
          " --praxik-dev-postgres Server=#{db_ip};" +
                                 "Port=#{port};" +
                                 "Uid=#{db_user_name};" +
                                 "Pwd=#{db_password};" +
                                 "Database=#{db_name};" +
          " --praxik-gis-server Server=#{geoserver_ip};" +
                               "Port=#{db_port};" +
                               "Uid=postgres;" +
                               "Pwd=postgres;" +
          " -j #{report_record_id}"

# Run the command we just set up, pushing its stdout and stderr back up the
# chain to the calling process. We also capture the command's exit status so
# this script can exit with the same status.
exit_status = 0
has_errors = false
already_pushed = []
push_threads = []
begin
  IO.popen(command,:err=>[:child, :out]) do |io|
    while !io.eof?
      msg = io.gets
      if msg =~ /^push_file/
        # Don't push the same file multiple times during a job.
        bucket_comma_filename = msg.sub!(/^push_file\s*:\s*/,'')
        if !already_pushed.include?(bucket_comma_filename)
          # FIXME: deal with credentials and logger for this.
          #push_threads << Thread.new(bucket_comma_filename, false, @credentials, @logger){ |b,d,c,l| EZQ.FilePusher.new(b,d,c,l) }
          already_pushed << bucket_comma_filename
        end
      elsif msg =~ /^error_message/
        puts msg # propagate up the chain
        has_errors = true
      else
        puts msg
      end
    end
    io.close
    exit_status = $?.to_i
  end
rescue => e
  exit_status = 1
  puts e
end

push_threads.each{|t| t.join()}

# The message written here will be picked up by EZQ::Processor and placed into
# the result queue specified in mmp_example_config.yml.
if exit_status.zero?
  result_message = {}
  result_message['report_record_id'] = report_record_id
  result_message['worker_done'] = true
  File.write(output_file,result_message.to_json)
end

exit(exit_status)