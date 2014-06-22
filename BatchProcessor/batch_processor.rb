#!/usr/bin/env ruby

require 'pg'
require 'json'
require 'yaml'
require 'aws-sdk'
require 'securerandom'

# This application polls the Batch queue, and uses the information given there
# to produce one or more JSON job blocks that are sent (individually) to the
# Job queue.

class BatchProcessor

def initialize(job_desc)
  # Pull connection string out of job_desc
  conn_str = job_desc['connection_string']
  fail_wtih("No connection string specified for BatchProcessor.") if !conn_str

  # Connect to db
  begin
    db = PG.connect(conn_str)
  rescue => e
    fail_with("Failed to connect to db with error: #{e}")
  end

  # Pull query out of job_desc
  query = job_desc['query']
  fail_with("No query sepcified in job description to BatchProcessor") if !query

  # Execute query
  result = db.exec(query)
  begin
    result.check()
  rescue => e
    fail_with("Database error: #{e}")
  end

  queue_name = job_desc['job_queue']
  fail_with("No job queue specified") if !queue_name

  pass_through = {}
  pass_through = job_desc.fetch('add_to_job_message_body',nil)

  msgs = []
  batch_type = job_desc['batch_type']
  case batch_type
  when 'multi'
    msgs = split_results(result,pass_through)
  when 'single'
    msgs = unify_results(result,pass_through)
  else
    fail_with("Invalid batch_type specified. Must be one of 'multi', 'single'")
  end

  # Get preamble overrides from job_desc
  preamble = {}
  preamble['EZQ'] = job_desc.fetch('job_preamble',nil)

  enqueue(msgs,preamble,queue_name)
end


def split_results(results,pass_through)
  # Array to contain each record as JSON. Each record will first be turned
  # into an array of one record, because Pregrid expects to iterate through an
  # array for each job.
  jobs = [] 
  results.each do |record|
    job = {}
    job['job'] = [record]
    job.merge!(pass_through) if pass_through
    jobs << job.to_json()
  end
  return jobs
end


def unify_results(results,pass_through)
  rs = [] # Array to contain entire recordset as Ruby structure
  results.each{|record| rs << record}
  job = {}
  job['job'] = rs
  job.merge!(pass_through) if pass_through
  return [job.to_json]
end


def enqueue(msg_ary,preamble,queue_name)
  queue = AWS::SQS.new.queues.named(queue_name)
  fail_with("Invalid queue specified: #{queue_name}") if !queue
  
  msg_ary.each_slice(10) do |msgs|
    #msgs.map{|msg| msg.insert(0,preamble.to_yaml)}
    msgs = msgs.map{|msg| set_preamble(msg,preamble)}
    queue.batch_send(*msgs)
  end
end


def set_preamble(body,preamble)
  if (body.bytesize + preamble.to_yaml.bytesize) > 256000  #256k limit minus assumed
                                                     #metadata size of 6k
                                                     #(256-6)*1024 = 256000
      body,preamble = divert_body_to_s3(body,preamble)
  end
  return body.insert(0,"#{preamble.to_yaml}...\n")
end


# This method is copied from EZQ::Processor
# Place message body in S3 and update the preamble accordingly
def divert_body_to_s3(body,preamble)
  #@logger.info 'Message is too big for SQS and is beig diverted to S3'
  # Don't assume the existing preamble can be clobbered
  new_preamble = preamble.clone
  s3 = AWS::S3.new
  bucket_name = 'EZQOverflow.praxik'
  bucket = s3.buckets[bucket_name]
  if !bucket
    #@log.fatal errm
    raise "Result overflow bucket #{bucket_name} does not exist!"
  end
  key = "overflow_body_#{SecureRandom.uuid}.txt"
  puts "Sending #{key} to S3"
  obj = bucket.objects.create(key,body)
  AWS.config.http_handler.pool.empty! # Hack to solve odd timeout issue
  s3_info = {'bucket'=>bucket_name,'key'=>key}
  new_preamble['EZQ'] = {} if new_preamble['EZQ'] == nil
  new_preamble['EZQ']['get_s3_file_as_body'] = s3_info
  body = "Message body was too big and was diverted to S3 as s3://#{bucket_name}/#{key}"
  return [body,new_preamble]
end


def fail_with(msg)
  puts "Fatal: #{msg}"
  exit(1)
end

end # class

################################################################################
if __FILE__ == $0

  require 'optparse'
  creds_file = 'credentials.yml'
  op = OptionParser.new do |opts|
    opts.banner = "Usage: batch_processor.rb [options] yaml_input_file"

    opts.on("-r", "--credentials [CREDS_FILE]","Use credentials file CREDS_FILE. Defaults to credentials.yml if not specified.") do |file|
      creds_file = file
    end
  end

  begin op.parse! ARGV
  rescue OptionParser::InvalidOption => e
    puts e
    puts op
    exit(1)
  end

  input_file = ARGV.shift
  if !input_file
    puts "Fatal: No input file specified."
    puts op
    exit(1)
  end

  AWS.config(YAML.load(File.read(creds_file)))

  # Read in the file named on commandline, parse as yaml, and fire off a BatchP.
  BatchProcessor.new(YAML.load(File.read(input_file)))

end
