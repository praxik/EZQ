#!/usr/bin/env ruby

require './deps/configulator'
require 'zlib'
require 'base64'

# Parses the config file
cf = Configulator.new

#queue = AWS::SQS.new.queues.named(cf.queue_name)
ops = {:visibility_timeout => 12}
queue = AWS::SQS.new.queues.create(cf.queue_name, ops)
# Set queue to use long polling
queue.wait_time_seconds= 20
puts "Sending messages to queue #{cf.queue_name}"


queue_type = ARGV[0]
use_compression = ARGV[1] == 'true' ? true : false

msgs_text = []
case queue_type
when 'raw'
  msgs_text = ['Hello 1', 'Hello 2', 'Hello 3', 'Hello 4']
  if use_compression
    puts "Using compression"
    msgs_text.map!{ |txt| Zlib::Deflate.deflate(txt,9) }
    msgs_text.map!{|item| Base64.encode64(item)}
  end
when 's3'
  m = {}
  m['bucket'] = cf.s3_bucket
  if use_compression
    m['key'] = 'Hello.txt.gz'
  else
    m['key'] = 'Hello.txt'
  end
  msgs_text = [m.to_yaml, m.to_yaml,m.to_yaml,m.to_yaml]
  # Create bucket and put the two test files in if needed
  s3 = AWS::S3.new
  bucket = s3.buckets[cf.s3_bucket]
  s3.buckets.create(cf.s3_bucket) unless bucket.exists?
  obj = bucket.objects['Hello.txt']
  bucket.objects[m['key']].write(Pathname.new(m['key'])) unless obj.exists?
when 'uri'
  m = {}
  m['uri'] = cf.uri
  msgs_text = [m.to_yaml, m.to_yaml,m.to_yaml,m.to_yaml]
end


msgs = queue.batch_send(msgs_text)

msgs.each do |msg|
  puts "Sent message: #{msg.id}"
end

