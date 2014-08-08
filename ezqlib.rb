require 'aws-sdk'
require 'securerandom'
require 'digest/md5'
require 'json'
require 'yaml'
require 'zip'
require 'zlib'
require 'base64'
require 'fileutils'


# Not sure I want to monkey-patch like this. It might be better to have
# a function rather than a method.
module Enumerable
  # This method cribbed from
  # http://rubydoc.info/github/svenfuchs/space/Enumerable:map_slice
  def map_slice(num, &block)
    result = []
    each_slice(num) { |element| result << yield(element) }
    return result
  end
end



module EZQ

  # Returns a new array of size +size+ which contains the elements of +ary+
  # repeated cyclically.
  def self.cyclical_fill( ary,size )
    elem = ary.cycle
    result = []
    size.times{result << elem.next}
    return result
  end



  # Diverts the body of a message intended for SQS off to S3. This is useful
  # when the message size exceeds SQS's limit.
  # @param [String] body The message body
  # @param [Hash] preamble The existing EZQ preamble has for the message
  # @param [String] bucket_name S3 bucket to use for overflow.
  #                 Defaults to 'Overflow'.
  # @param [String] key The S3 key to use. Defaults to a random uuid.
  #
  # @returns [Array(String,String)] Returns the new body and new preamble that
  #                                 should be sent on to a queue in place of the
  #                                 original.
  def self.divert_body_to_s3( body,
                              preamble,
                              bucket_name = 'Overflow',
                              key = SecureRandom.uuid() )
    bucket_name = 'EZQ.Overflow' if bucket_name == nil
    key = SecureRandom.uuid if key == nil
    new_preamble = preamble.clone
    send_data_to_s3( body,bucket_name,key )
    new_preamble['EZQ'] = {} if !new_preamble.has_key?('EZQ') or new_preamble['EZQ'] == nil
    new_preamble['EZQ']['get_s3_file_as_body'] = {'bucket'=>bucket_name,'key'=>key}
    body = "Message body was diverted to S3 as s3://#{bucket_name}/#{key}"
    return [body,new_preamble]
  end



  # Enqueues an array of messages in SQS, automatically diverting each body to
  # S3 if it is larger than SQS allows.
  # @param [String,Array] body_ary Array of message bodies
  # @param [Hash,Array] preamble Array of EZQ preambles to use for the messages.
  #                              If this array is smaller than body_ary,
  #                              the elements in it will be repeated cyclically
  #                              to match up with body_ary.
  # @param [AWS::SQS::Queue,String] queue SQS::Queue object or name of an SQS
  #                                       queue
  # @param [Bool] create_queue_if_needed Create the queue if it doesn't exist?
  # @param [String] bucket Name of S3 bucket to use for overflow if
  #                        message is too large for SQS
  # @param [String,Array] key_ary Array of S3 keys to use if message is too
  #                               large for
  #                               SQS.
  # @return [String,Array] Array of MD5 hexdigests of the sent messages. The
  #                        strings will be empty if the messages failed to send.
  def self.enqueue_batch( body_ary,
                          preamble_ary,
                          queue,
                          create_queue_if_needed = false,
                          bucket = nil,
                          key_ary = [] )
    q = get_queue(queue,create_queue_if_needed)

    # Just to be safe; should never happen in practice.
    return ['']*body_ary.size if !q.respond_to?( :send_message )

    preambles = cyclical_fill( Array(preamble_ary),body_ary.size )
    keys = fill_array( key_ary,body_ary.size,nil )
    msgs = body_ary.zip( preambles ).zip( keys ).flatten.
      map_slice(3){|body,preamble,key| prepare_message_for_queue( body,preamble,bucket,key )}
    
    digests = msgs.map{|msg| Digest::MD5.hexdigest( msg )}

    mdfives = []
    msgs.each_slice(10){|batch| mdfives += q.batch_send( *batch ).map{|sent| sent.md5}}
    return digests if digests == mdfives
    # If any one message digest doesn't match, behave as though the whole
    # batch failed (because it probably did!)
    return ['']*body_ary.size
  end



  # Enqueues a message in SQS, automatically diverting the body to S3
  # if it is larger than SQS allows.
  # @param [String] body The message body
  # @param [Hash] preamble The EZQ preamble to use for this message
  # @param [String] queue Name of the SQS queue
  # @param [Bool] create_queue_if_needed Create the queue if it doesn't exist?
  # @param [String] bucket Name of S3 bucket to use for overflow if
  #                        message is too large for SQS
  # @param [String] key S3 key to use if message is too large for SQS
  # @return [String] MD5 hexdigest of the sent message. The return string will
  #                  be empty if the message failed to send.
  def self.enqueue_message( body,
                            preamble,
                            queue,
                            create_queue_if_needed = false,
                            bucket = nil,
                            key = nil )
    return enqueue_batch( [body],[preamble],queue,create_queue_if_needed,
                          bucket,[key] ).first
  end



  # Returns an array containing up to +size+ elements of +ary+. If +ary+ is
  # smaller than +size+, the remainder of the returned array contains +filler+.
  # @param [Array] ary Input array from which to draw elements
  # @param [Integer] size  Size of array to return
  # @param [Object] filler The fill object to use to pad the end of the array
  #                        if +ary+ is smaller than +size+.
  def self.fill_array( ary,size,filler )
    if ary.size < size
      return ary + Array.new( size-ary.size,filler )
    else
      return ary.take( size )
    end
  end



  # Returns the desired queue, creating it if necessary and requested. Returns
  # nil if the queue could not be created.
  # @param [AWS::SQS::Queue,String] queue SQS::Queue object or name of queue
  # @param [Bool] create_if_needed Create the queue if it doesn't exist?
  # @returns [AWS::Queue] Returns the requested queue, or nil.
  # @raise Raises exception if the queue could not be created when it didn't
  #        already exist and create_if_needed was true.
  def self.get_queue( queue,create_if_needed=false )

    # If it's an SQS::Queue already, treat it as such....
    if queue.respond_to?( :send_message )
      return queue if queue.exists?
      return AWS::SQS.new.queues.create(queue.name) if create_if_needed
      return nil
    end

    # Nope, it's a string.
    # Accidental whitespace at the end of the name causes a horribly subtle bug
    queue = queue.strip
    sqs = AWS::SQS.new
    begin
      return sqs.queues.named( queue )
    rescue
      return sqs.queues.create( queue ) if create_if_needed
      return nil
    end
  end



  # Transforms a separate body and preamble into a single message suitable for
  # SQS. If the body + preamble is too big to fit in SQS, this method will
  # automatically divert the body to S3 and return a message containig a new
  # (smaller) body with a mutated preamble.
  # @param [String] body Body for the message
  # @param [Hash] preamble The EZQ preamble hash for this message
  # @param [String] bucket Name of S3 bucket to use for message that are too
  #                        large for SQS.
  # @param [String] key S3 key name to use if message is too large for SQS
  # @return [String] The full message with the preamble and body concatenated
  #                  together. Both will have been altered if the body had to
  #                  be diverted to S3.
  def self.prepare_message_for_queue( body,preamble,bucket=nil,key=nil )
    # 256k limit minus assumed metadata size of 6k:  (256-6)*1024 = 256000
    bc = body.clone
    if (body.bytesize + preamble.to_yaml.bytesize) > 256000   
      body,preamble = divert_body_to_s3( body,preamble,bucket,key )
    end

    return bc.insert( 0,"#{preamble.to_yaml}...\n" )
  end



  # Sends data to S3 using bucket_name and key
  # @param [String] data Data to send
  # @param [String] bucket_name S3 Bucket. If the bucket doesn't already exist,
  #                             it will be automatically created.
  # @param [String] key S3 key to use.
  # @return [nil] Always returns nil
  def self.send_data_to_s3( data,bucket_name,key )
    thread = send_data_to_s3_async( data,bucket_name,key )
    thread.join
    return nil
  end


  # Sends data to S3 on a new thread, using bucket_name and key
  # @param [String] data Data to send
  # @param [String] bucket_name S3 Bucket. If the bucket doesn't already exist,
  #                             it will be automatically created.
  # @param [String] key S3 key to use.
  # @return [Thread] Returns a handle to a Thread. You should ensure that
  #                  Thread#join is called on this thread before existing your
  #                  application.
  def self.send_data_to_s3_async( data,bucket,key )
    return Thread.new(data,bucket,key){ |d,b,k| DataPusher.new(d,b,k) }
  end


  # Sends a file to S3, using bucket_name and key
  # @param [String] filename Path of file to send
  # @param [String] bucket_name S3 Bucket. If the bucket doesn't already exist,
  #                             it will be automatically created.
  # @param [String] key S3 key to use.
  # @return [nil] Always returns nil.
  def self.send_file_to_s3( filename,bucket,key )
    thread = send_file_to_s3_async( filename,bucket,key )
    thread.join
    return nil
  end


  # Sends a file to S3 on a new thread, using bucket_name and key
  # @param [String] filename Path of file to send
  # @param [String] bucket_name S3 Bucket. If the bucket doesn't already exist,
  #                             it will be automatically created.
  # @param [String] key S3 key to use.
  # @return [Thread] Returns a handle to a Thread. You should ensure that
  #                  Thread#join is called on this thread before existing your
  #                  application.
  def self.send_file_to_s3_async( filename,bucket,key )
    return Thread.new(filename,bucket,key){ |f,b,k| FilePusher.new(f,b,k) }
  end


  # Sends a file to S3, using the filename as the key.
  # @param [String] bucket_commna_filename Bucket and filename (key) joined with
  #                                        a comma and no spaces. The bucket
  #                                        will be created if it doesn't exist.
  # @return [nil] Always returns nil.
  def self.send_bcf_to_s3( bucket_comma_filename )
    thread = send_bcf_to_s3( bucket_comma_filename )
    thread.join
    return nil
  end


  # Sends a file to S3, using the filename as the key.
  # @param [String] bucket_commna_filename Bucket and filename (key) joined with
  #                                        a comma and no spaces. The bucket
  #                                        will be created if it doesn't exist.
  # @return [Thread] Returns a handle to a Thread. You should ensure that
  #                  Thread#join is called on this thread before existing your
  #                  application.
  def self.send_bcf_to_s3_async( bucket_comma_filename )
    bucket,key = bucket_comma_filename.split(',').map{|s| s.strip}
    return send_file_to_s3_async( key,bucket,key )
  end



  # DataPusher class pushes specified data to S3. It is intended to be used
  # as a thread object.
  class DataPusher
    def initialize( data,bucket_name,key )
      s3 = AWS::S3.new
      s3.buckets.create( bucket_name ) if !s3.buckets[bucket_name].exists?
      bucket = s3.buckets[bucket_name]
      obj = bucket.objects.create(key,data)
      AWS.config.http_handler.pool.empty! # Hack to solve s3 timeout issue
      return nil
    end
  end



  # FilePusher class pushes specified file to S3. It is intended to be used
  # as a thread object.
  class FilePusher
    def initialize( filename,bucket_name,key )
      if File.exists?(filename)
        s3 = AWS::S3.new
        s3.buckets.create( bucket_name ) if !s3.buckets[bucket_name].exists?
        bucket = s3.buckets[bucket_name]
        obj = bucket.objects.create(key,Pathname.new(filename))
        AWS.config.http_handler.pool.empty! # Hack to solve s3 timeout issue
      else
        raise "File '#{filename}' does not exist."
      end
      return nil
    end
  end


  # Un-escapes an escaped string. Cribbed from
  # http://stackoverflow.com/questions/8639642/whats-the-best-way-to-escape-and-unescape-strings
  # 
  def unescape(str)
    str.gsub(/\\(?:([#{UNESCAPES.keys.join}])|u([\da-fA-F]{4}))|\\0?x([\da-fA-F]{2})/) {
      if $1
        if $1 == '\\' then '\\' else UNESCAPES[$1] end
      elsif $2 # escape \u0000 unicode
        ["#$2".hex].pack('U*')
      elsif $3 # escape \0xff or \xff
        [$3].pack('H2')
      end
    }
  end

  UNESCAPES = {
    'a' => "\x07", 'b' => "\x08", 't' => "\x09",
    'n' => "\x0a", 'v' => "\x0b", 'f' => "\x0c",
    'r' => "\x0d", 'e' => "\x1b", "\\\\" => "\x5c",
    "\"" => "\x22", "'" => "\x27"}


  # Decompresses the file and stores the result in a file with the same name.
  def self.decompress_file(filename)
    Zip::File.open(filename) do |zip_file|
      zip_file.each { |entry| entry.extract(entry.name) }
    end
  end


  # Decompress a file that contains data compressed directly with libz; that
  # is, the file is not a standard .zip with appropriate header information.
  def self.decompress_headerless_file(filename)
    File.open(filename) do |cf|
      zi = Zlib::Inflate.new(Zlib::MAX_WBITS + 32)
      uncname = filename + '.uc'
      File.open(uncname, "w+") {|ucf|
      ucf << zi.inflate(cf.read) }
      zi.close
    end
    File.delete(filename)
    File.rename(filename + '.uc', filename)
  end


  # Compresses the file and stores the result in filename.gz
  def self.compress_file(filename)
    Zlib::GzipWriter.open("#{filename}.gz",9) do |gz|
      gz.mtime = File.mtime(filename)
      gz.orig_name = filename
      gz.write IO.binread(filename)
    end
  end


  # Returns a new string with the **first** EZQ preamble stripped off
  def self.strip_preamble(str)
    return str.sub(/-{3}\nEZQ.+?\.{3}\n/m,'')
  end


  # Replaces the body of an AWS::SQS::RecievedMessage with a version of the
  # body that doesn't contain the **first** EZQ preamble. Returns nil.
  def self.strip_preamble_msg!(msg)
    msg.body.sub!(/-{3}\nEZQ.+?\.{3}\n/m,'')
    return nil
  end

  def self.extract_preamble(msgbody)
    body = YAML.load(msgbody)
    return '' if !body.kind_of?(Hash)
    return '' if !body.has_key?('EZQ')
    return body['EZQ']
  rescue
    return ''
  end

  # Rogue out single backslashes that are not real escape sequences and
  # turn them into double backslashes.
  def self.fix_escapes(text)
    # (?<!\\)  -- no backslashes directly before current match
    # (\\)     -- match a single backslash
    # (?![\\\/\"\'rnbt])  -- not followed by a character that would indicate
    #                        this is already a valid escape sequence:
    #                        backslash, forwardslash, double quote,
    #                        single quote, r, n, b, or t
    # "\\\\\\\\" -- it takes *8* backslashes to indicate two backslashes: one
    #               pass of escaping for the regexp (\\ --> \\\\) and a second
    #               pass of escaping for the ruby string (\\\\ --> \\\\\\\\)
    return text.gsub(/(?<!\\)(\\)(?![\\\/\"\'rnbt])/,"\\\\\\\\")
  end

end


################################################################################
################################################################################
################################################################################
################################################################################
# Unit Tests
if __FILE__ == $0

print 'Setting up AWS...'
AWS.config(YAML.load(File.read('credentials.yml')))
puts 'done'

# Send a message to a queue, ask to receive it back, check it.
def test_one
  digest = EZQ.enqueue_message( "Test data", {'EZQ'=>nil},'test_queue',true )
  print "Test one (enqueue a single message): "
  q = EZQ.get_queue('test_queue')
  if !q
    puts "failed to get test_queue"
    return nil
  end
  q.receive_message do |msg|
    if msg.md5 == digest
      puts "pass"
    else
      puts "fail"
    end
  end
  return nil
end

# Send a message that we know is way too big to a queue. Ensure it was diverted
# to S3 properly. Ask to receive message back from queue, and check the
# preamble.
def test_two
  digest = EZQ.enqueue_message((1..40000).to_a.to_yaml,{'EZQ'=>nil},'test_queue')
  print "Test two (enqueue an oversized message): "
  q = EZQ.get_queue('test_queue')
  if !q
    puts "failed to get test_queue"
    return nil
  end
  q.receive_message do |msg|
    pre = YAML.load(msg.body)
    if pre.has_key?('get_s3_file_as_body')
      puts 'pass'
    else
      puts 'fail'
    end
  end
  return nil
end


# Send a whole batch of messages at once.
def test_three
  print "Test three (enqueue batch of messages): "
  msg_ary = (1..14).map{|i| i.to_s}
  EZQ.enqueue_batch(msg_ary,[{'EZQ'=>nil}],'test_queue')
  q = EZQ.get_queue('test_queue')
  14.times do
    q.receive_message do |msg|
      msg_ary -= [EZQ.strip_preamble(msg.body)]
    end
  end
  puts msg_ary
  if msg_ary.empty?
    puts 'pass'
  else
    puts 'fail'
  end
end


def test_four
  print "Test four (send a file to s3 using bcf notation): "
  File.write('test.txt',"This is a test")
  thread = EZQ.send_bcf_to_s3_async("6k_test.praxik,test.txt")
  thread.join
  puts 'pass'
end

#test_one()
#test_two()
#test_three()
#test_four()


end
