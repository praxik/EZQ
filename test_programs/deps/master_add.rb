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
  msgs_text = ['raw_1.txt', 'raw_2.txt', 'raw_3.txt', 'raw_4.txt']
  msgs_text.map!{|fname| File.read(File.join(File.dirname(__FILE__),fname))}
  if use_compression
    puts "Using compression"
    msgs_text.map!{ |txt| Zlib::Deflate.deflate(txt,9) }
    msgs_text.map!{|item| Base64.encode64(item)}
  end
when 's3'
  msgs_text = ['s3.txt','s3.txt','s3.txt','s3.txt']
  msgs_text.map!{|fname| File.read(File.join(File.dirname(__FILE__),fname))}
  # Create bucket and put the two test files in if needed
  s3 = AWS::S3.new
  bucket = s3.buckets[cf.s3_bucket]
  s3.buckets.create(cf.s3_bucket) unless bucket.exists?
  str = msgs_text[0]
  m = YAML.load(str)
  m['EZQ']['get_s3_files'].each do |file|
    obj = bucket.objects[file['key']]
    bucket.objects[file['key']].write(Pathname.new(File.join(File.dirname(__FILE__),file['key']))) unless obj.exists?
  end
when 'uri'
  msgs_text = ['uri.txt','uri.txt','uri.txt','uri.txt']
  msgs_text.map!{|fname| File.read(File.join(File.dirname(__FILE__),fname))}
end


msgs = queue.batch_send(msgs_text)

msgs.each do |msg|
  puts "Sent message: #{msg.id}"
end

