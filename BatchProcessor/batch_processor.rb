#!/usr/bin/env ruby

require 'pg'
require 'json'
require 'yaml'
require 'aws-sdk'

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
  preamble_string = preamble.to_yaml + "...\n"

  enqueue(msgs,preamble_string,queue_name)

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
  results.each do {|record| rs << record}
  job = {}
  job['job'] = rs
  job.merge!(pass_through) if pass_through
  return [job.to_json]
end


def enqueue(msg_ary,preamble_string,queue_name)
  queue = AWS::SQS.new.queues.named(queue_name)
  fail_with("Invalid queue specified: #{queue_name}") if !queue
  
  msg_ary.each_slice(10) do |msgs|
    msgs.map{|msg| msg.insert(0,preamble_string)}
    queue.batch_send(*msgs)
  end
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
