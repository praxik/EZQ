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

  @log = nil

  # Sets the logger for ezqlib calls. Pass in something that reponds like
  # Ruby's builtin Logger class and you're good to go.
  # @param [Logger-like] logger The Logger to use. Must respond to Logger
  #   methods and Logger level constants, but need not be derived from Logger.
  def EZQ.set_logger(logger)
    @log = logger
  end

  # Returns the local ip of the machine using only standard builtin OS calls.
  # Restricted to Windows and Unix-like OSes.
  def EZQ.get_local_ip()
    ip = ''
    if RUBY_PLATFORM =~ /mswin|mingw/
      cmd = 'FOR /f "tokens=1 delims=:" %d IN (\'ping %computername% -4 -n 1 ^| find /i "reply"\') DO FOR /F "tokens=3 delims= " %g IN ("%d") DO echo %g'
    else
      cmd = 'ifconfig eth0 | sed -n "2s/[^:]*:[ \t]*\([^ ]*\) .*/\1/p"'
    end
    IO.popen(cmd) do |io|
      while !io.eof?
        ip << io.gets
      end
      io.close
    end
    return ip.split().last()
  end

  # Returns a new array of size +size+ which contains the elements of +ary+
  # repeated cyclically.
  def EZQ.cyclical_fill( ary,size )
    elem = ary.cycle
    result = []
    size.times{result << elem.next}
    return result
  end


  # Returns handle to S3.
  # @param [Bool] reset If true, get a new S3 object rather than
  #                     returning the cached one
  # @return [AWS::S3] the S3 object
  def EZQ.get_s3(reset: false)
    @s3 = nil if reset
    # The backoff retry is an attempt to deal with this uninformative error:
    # `block in add_service': undefined method `global_endpoint?' for AWS::S3:
    #  Class (NoMethodError)
    @s3 ||= exceptional_retry_with_backoff(5,1,1){AWS::S3.new()}

    while !@s3.respond_to?(:buckets)
      @log.warn "AWS::S3 isn't responding to 'buckets'. Again." if @log
      @s3 = exceptional_retry_with_backoff(5,1,1){AWS::S3.new()}
    end
    return @s3
  end


  # Diverts the body of a message intended for SQS off to S3. This is useful
  # when the message size exceeds SQS's limit.
  # @param [String] body The message body
  # @param [Hash] preamble The existing EZQ preamble has for the message
  # @param [String] bucket_name S3 bucket to use for overflow.
  #                 Defaults to 'Overflow'.
  # @param [String] key The S3 key to use. Defaults to a random uuid.
  #
  # @return [Array(String,String)] Returns the new body and new preamble that
  #                                 should be sent on to a queue in place of the
  #                                 original.
  def EZQ.divert_body_to_s3( body,
                              preamble,
                              bucket_name = 'Overflow',
                              key = SecureRandom.uuid() )
    @log.debug "EZQ::divert_body_to_s3" if @log
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
  def EZQ.enqueue_batch( body_ary,
                          preamble_ary,
                          queue,
                          create_queue_if_needed = false,
                          bucket = nil,
                          key_ary = [] )
    @log.debug "EZQ::enqueue_batch" if @log
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
    @log.warn "EZQ::enqueue_batch: digests didn't match" if @log
    return ['']*body_ary.size
  end



  # Enqueues a message in SQS, automatically diverting the body to S3
  # if it is larger than SQS allows.
  # @param [String] body The message body
  # @param [Hash] preamble The EZQ preamble to use for this message
  # @param [AWS::SQS::Queue,String] queue SQS::Queue object or name of an SQS
  #                                       queue
  # @param [Bool] create_queue_if_needed Create the queue if it doesn't exist?
  # @param [String] bucket Name of S3 bucket to use for overflow if
  #                        message is too large for SQS
  # @param [String] key S3 key to use if message is too large for SQS
  # @return [String] MD5 hexdigest of the sent message. The return string will
  #                  be empty if the message failed to send.
  def EZQ.enqueue_message( body,
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
  def EZQ.fill_array( ary,size,filler )
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
  def EZQ.get_queue( queue,create_if_needed=false )
    @log.debug "EZQ::get_queue" if @log
    # If it's an SQS::Queue already, treat it as such....
    if queue.respond_to?( :send_message )
      begin
        return queue if queue.exists?
      rescue(AWS::SQS::Errors::ExpiredToken)
        return EZQ.get_queue( queue.name, create_if_needed)
      end
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
  def EZQ.prepare_message_for_queue( body,preamble,bucket=nil,key=nil )
    @log.debug "EZQ::prepare_message_for_queue" if @log
    # 256k limit minus assumed metadata size of 6k:  (256-6)*1024 = 256000
    bc = body.clone
    if (body.bytesize + preamble.to_yaml.bytesize) > 256000
      bc,preamble = divert_body_to_s3( bc,preamble,bucket,key )
    end

    return bc.insert( 0,"#{preamble.to_yaml}...\n" )
  end



  # Sends data to S3 using bucket_name and key
  # @param [String] data Data to send
  # @param [String] bucket_name S3 Bucket. If the bucket doesn't already exist,
  #                             it will be automatically created.
  # @param [String] key S3 key to use.
  # @param [Bool] compress If true, compresses data with gzip and alters key to end
  #               with '.gz'. Default: false. Note this is a **named parameter**.
  # @return [nil] Always returns nil
  def EZQ.send_data_to_s3( data,bucket_name,key,compress: false )
    thread = send_data_to_s3_async( data,bucket_name,key,compress: compress )
    thread.join
    return nil
  end



  # Sends data to S3 on a new thread, using bucket_name and key
  # @param [String] data Data to send
  # @param [String] bucket_name S3 Bucket. If the bucket doesn't already exist,
  #                             it will be automatically created.
  # @param [String] key S3 key to use.
  # @param [Bool] compress If true, compresses data with gzip and alters key to end
  #               with '.gz'. Default: false. Note this is a **named parameter**.
  # @return [Thread] Returns a handle to a Thread. You should ensure that
  #                  Thread#join is called on this thread before existing your
  #                  application.
  def EZQ.send_data_to_s3_async( data,bucket,key,compress: false )
    return Thread.new(data,bucket,key,compress){ |d,b,k,c| DataPusher.new(d,b,k,c,@log) }
  end



  # Sends a file to S3, using bucket_name and key
  # @param [String] filename Path of file to send
  # @param [String] bucket_name S3 Bucket. If the bucket doesn't already exist,
  #                             it will be automatically created.
  # @param [String] key S3 key to use.
  # @param [Hash] options An S3 object options hash. See docs for AWS::S3::S3Object#write
  # @param [Bool] compress If true, compresses file with gzip and alters key to end
  #               with '.gz'. Default: false. Note this is a **named parameter**.
  # @return [nil] Always returns nil.
  def EZQ.send_file_to_s3( filename,bucket,key,options: {},compress: false )
    thread = send_file_to_s3_async( filename,bucket,key,options: options,compress: compress )
    thread.join
    return nil
  end



  # Sends a file to S3 on a new thread, using bucket_name and key
  # @param [String] filename Path of file to send
  # @param [String] bucket_name S3 Bucket. If the bucket doesn't already exist,
  #                             it will be automatically created.
  # @param [String] key S3 key to use.
  # @param [Hash] options An S3 object options hash. See docs for AWS::S3::S3Object#write
  # @param [Bool] compress If true, compresses file with gzip and alters key to end
  #               with '.gz'. Default: false. Note this is a **named parameter**.
  # @return [Thread] Returns a handle to a Thread. You should ensure that
  #                  Thread#join is called on this thread before exiting your
  #                  application.
  def EZQ.send_file_to_s3_async( filename,bucket,key,options: {},compress: false )
    return Thread.new(filename,bucket,key,options,compress){ |f,b,k,c| FilePusher.new(f,b,k,options,c,@log) }
  end



  # Sends a file to S3, using the filename as the key.
  # @param [String] bucket_comma_filename Bucket and filename (key) joined with
  #                                       a comma and no spaces. The bucket
  #                                       will be created if it doesn't exist.
  # @param [Hash] options An S3 object options hash. See docs for AWS::S3::S3Object#write
  # @param [Bool] compress If true, compresses file with gzip and alters key to end
  #               with '.gz'. Default: false. Note this is a **named parameter**.
  # @return [nil] Always returns nil.
  def EZQ.send_bcf_to_s3( bucket_comma_filename,options: {},compress: false )
    thread = send_bcf_to_s3_async( bucket_comma_filename,options: options,compress: compress )
    thread.join
    return nil
  end



  # Sends a file to S3, using the filename as the key.
  # @param [String] bucket_comma_filename Bucket and filename (key) joined with
  #                                       a comma and no spaces. The bucket
  #                                       will be created if it doesn't exist.
  # @param [Hash] options An S3 object options hash. See docs for AWS::S3::S3Object#write
  # @param [Bool] compress If true, compresses file with gzip and alters key to end
  #               with '.gz'. Default: false. Note this is a **named parameter**.
  # @return [Thread] Returns a handle to a Thread. You should ensure that
  #                  Thread#join is called on this thread before exiting your
  #                  application.
  def EZQ.send_bcf_to_s3_async( bucket_comma_filename, options: {},compress: false )
    bucket,key = bucket_comma_filename.split(',').map{|s| s.strip}
    return send_file_to_s3_async( key,bucket,key,options: options,compress: compress )
  end



  # DataPusher class pushes specified data to S3. It is intended to be used
  # as a thread object.
  class DataPusher
    def initialize( data,bucket_name,key,compress=false,log=nil )
      @retries ||= 10
      if compress
        sio = StringIO.new
        gz = Zlib::GzipWriter.new(sio,9)
        gz.write(data)
        gz.close
        data = sio.string
        key = "#{key}.gz" if !(key =~ /\.gz$/)
      end

      s3 = EZQ.get_s3()
      s3.buckets.create( bucket_name ) if !s3.buckets[bucket_name].exists?
      bucket = s3.buckets[bucket_name]
      obj = bucket.objects[key]
      dig = Digest::MD5.hexdigest(data)
      if obj.exists? and ((obj.etag() == dig) or (obj.metadata.to_h.fetch('md5','') == dig ))
        log.debug "Remote file is up-to-date; skipping send." if log
        return nil
      end
      obj.write(data,{:metadata=>{:md5=>dig}})
      AWS.config.http_handler.pool.empty! # Hack to solve s3 timeout issue
      return nil
    rescue(AWS::S3::Errors::ExpiredToken)
      EZQ.get_s3(reset: true)
      retry
    rescue => e
      log.warn "EZQ::DataPusher: #{e}" if log
      retry if (@retries -= 1) > -1
      log.error "EZQ::DataPusher: #{e}" if log
      raise e
    end
  end



  # FilePusher class pushes specified file to S3. It is intended to be used
  # as a thread object.
  class FilePusher
    # @param [Hash] options An S3 object options hash. See docs for AWS::S3::S3Object#write
    def initialize( filename,bucket_name,key,options={},compress=false,log=nil )
      @retries ||= 10
      if !File.exists?(filename)
        raise "File '#{filename}' does not exist."
      end

      fname = filename

      # If filename doesn't end in .gz but key does, we will compress
      # *even if* +compress+ *is false*
      gz = /\.gz$/
      silent_compress = ( !(filename =~ gz) && key =~ gz )
      log "EZQ::FilePusher: implicitly gzipping #{filename}" if (log && silent_compress)
      compress |= silent_compress
      if compress
        fname = EZQ.compress_file(filename)
        key = "#{key}.gz" if !(key =~ gz)
      end

      s3 = EZQ.get_s3()
      s3.buckets.create( bucket_name ) if !s3.buckets[bucket_name].exists?
      bucket = s3.buckets[bucket_name]
      obj = bucket.objects[key]
      file_dig = EZQ.md5file(fname).hexdigest
      if obj.exists? and ((obj.etag() == file_dig) or (obj.metadata.to_h.fetch('md5','') == file_dig ))
        log.debug "Remote file is up-to-date; skipping send." if log
        return nil
      end
      md5_opts = {:metadata=>{:md5=>file_dig}}
      all_opts = options.merge(md5_opts)
      obj.write(Pathname.new(fname),all_opts)
      AWS.config.http_handler.pool.empty! # Hack to solve s3 timeout issue

      # Remove the temporary if we created one.
      File.unlink(key) if compress
      return nil
    rescue(AWS::S3::Errors::ExpiredToken)
      EZQ.get_s3(reset: true)
      retry
    rescue => e
      log.warn "EZQ::FilePusher: #{e}" if log
      retry if (@retries -= 1) > -1
      log.error "EZQ::FilePusher: #{e}" if log
      raise e
    end
  end


  # Pulls a single file down from S3. This method checks for the existence of
  # a local file with the same name as the one in S3. If the local file
  # exists and its md5 is the same as the file in S3, the method returns true
  # without actually downloading the file.
  # @param [String] bucket The S3 bucket from which to pull
  # @param [String] key The S3 key, which will also map directly to the local filename
  # @param [Bool] decompress Whether to decompress the file (.gz or .zip). Default: false
  #               Note this is a **named** parameter.
  # @param [Bool] keep_name If true and +decompress+ is true, decompressed file
  #               will have the same name as compressed file, including .gz or .zip
  #               extension. Note this is a **named** parameter.
  # @return [Bool] true if successful, false otherwise
  def EZQ.get_s3_file(bucket,key,decompress: false, keep_name: false)
    @log.debug "EZQ::get_s3_file '#{bucket}/#{key}'" if @log
    s3 = EZQ.get_s3()
    b = s3.buckets[ bucket ]
    obj = b.objects[ key ]
    FileUtils.mkdir_p(File.dirname(key))

    # Do we already have a current version of this file?
    if File.exists?(key) and (obj.etag() == EZQ.md5file(key).hexdigest)
      @log.debug "EZQ::get_s3_file: local file is already current" if @log
      return true
    end
    File.open(key,'wb'){ |f| obj.read {|chunk| f.write(chunk)} }

    if decompress
      type = File.extname(key)
      case type
      when '.gz'
        @log.debug "EZQ::get_s3_file: decompressing file with gunzip" if @log
        EZQ.gunzip(key,keep_name: keep_name)
      when '.zip'
        @log.debug "EZQ::get_s3_file: decompressing file with unzip" if @log
        EZQ.decompress_file(key)
      end
    end

    return true
  rescue(AWS::S3::Errors::ExpiredToken)
      @s3 = nil
      retry
  rescue
      return false
  end


  # Removes a file from S3. This will only remove individual files,
  # not complete "directories"
  # @param [String] bucket S3 bucket in which the file is stored
  # @param [String] key S3 key to the file
  # @return nil
  def EZQ.remove_s3_file(bucket,key)
    @log.debug "EZQ::remove_s3_file: #{bucket}/#{key}" if @log
    s3 = EZQ.get_s3()
    b = s3.buckets[ bucket ]
    obj = b.objects[ key ]
    obj.delete if obj.exists?
    return nil
  end


  # Get md5 of a file without having to read entire file into memory at once.
  # From https://www.ruby-forum.com/topic/58563
  # @param [String] filename The name of the file to digest.
  # @return [Digest] An md5 digest object.
  def EZQ.md5file(filename)
    md5 = File.open(filename, 'rb') do |io|
      dig = Digest::MD5.new
      buf = ""
      dig.update(buf) while io.read(4096, buf)
      dig
    end
    return md5
  end


  # Un-escapes an escaped string. Cribbed from
  # http://stackoverflow.com/questions/8639642/whats-the-best-way-to-escape-and-unescape-strings
  # Does *not* modify str in place. Returns a new, unescaped string.
  def EZQ.unescape(str)
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


  # Decompress a .zip archive
  def EZQ.decompress_file(filename, overwrite: true)
    Zip.on_exists_proc = true # Don't raise if extracted files already exist
    Zip::File.open(filename) do |zip_file|
      zip_file.each { |entry| entry.extract(entry.name) if (overwrite || !File.exists?(entry.name)) }
    end
  end


  # Decompress a file that contains data compressed directly with libz; that
  # is, the file is not a standard .zip with appropriate header information.
  # Decompresses the file and stores the result in a file with the same name.
  def EZQ.decompress_headerless_file(filename)
    uncname = filename + '.uc'
    File.open(filename) do |cf|
      zi = Zlib::Inflate.new(Zlib::MAX_WBITS + 32)
      File.open(uncname, "w+") {|ucf| ucf << zi.inflate(cf.read) }
      zi.close
    end
    File.delete(filename)
    File.rename(uncname, filename)
    return filename
  end


  def EZQ.gunzip(filename,keep_name: false)
    return nil if !File.extname(filename) == '.gz'
    uncname = ''
    File.open(filename) do |cf|
      zi = Zlib::Inflate.new(Zlib::MAX_WBITS + 32)
      # Strip .gz from the end of the filename
      uncname = filename.gsub(/\.gz$/,'')
      File.open(uncname, "w+") {|ucf| ucf << zi.inflate(cf.read) }
      zi.close
    end

    if keep_name
      File.delete(filename)
      File.rename(uncname,filename)
      return filename
    end
    return uncname
  end


  # Compresses the file and stores the result in filename.gz
  # @param [String] filename Path to file to compress
  # @return [String] Path to the compressed file
  def EZQ.compress_file(filename)
    cfname = "#{filename}.gz"
    Zlib::GzipWriter.open(cfname,9) do |gz|
      gz.mtime = File.mtime(filename)
      gz.orig_name = filename
      gz.write IO.binread(filename)
    end
    return cfname
  end


  # Returns a new string with the **first** EZQ preamble stripped off
  def EZQ.strip_preamble(str)
    return str.sub(/-{3}\nEZQ.+?\.{3}\n/m,'')
  end


  # Replaces the body of an AWS::SQS::RecievedMessage with a version of the
  # body that doesn't contain the **first** EZQ preamble. Returns nil.
  def EZQ.strip_preamble_msg!(msg)
    msg.body.sub!(/-{3}\nEZQ.+?\.{3}\n/m,'')
    return nil
  end


  # Returns a hash of the EZQ preamble. Does **not** remove the
  # preamble from the message body.
  def EZQ.extract_preamble(msgbody)
    body = YAML.load(msgbody)
    return '' if !body.kind_of?(Hash)
    return '' if !body.has_key?('EZQ')
    return body['EZQ']
  rescue
    return {}
  end

  # Rogue out single backslashes that are not real escape sequences and
  # turn them into double backslashes.
  def EZQ.fix_escapes(text)
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


  # Retries a block that raises exceptions, using a timed backoff. If all
  # retries fail, the last raised exception is raised by this function. If
  # the block succeeds, the return value of the block is returned, so this
  # method is safe to use as the rhs of an assignment.
  # @param [Integer] retries The number of times to retry the block.
  # @param [Integer,Float] first_delay Number of seconds to delay between
  #                                       the first failure and the first retry.
  # @param [Integer,Float] base The base to use when calculating successive
  #                             delays according to the formula
  #                             delay = first_delay/base * (base ** num_tries),
  #                             where num_tries increments monotonically from 1.
  #                             The default base is Math.exp(1); that is, 'e',
  #                             so that the default backoff is exponential. To
  #                             use a constant delay between retries, set base
  #                             equal to 1.
  def EZQ.exceptional_retry_with_backoff(retries,
                                          first_delay=1,
                                          base=Math.exp(1),
                                          &block)
    a = Float(first_delay) / Float(base)
    tries = 0
    result = nil
    begin
      tries += 1
      result = block.call()
    rescue => e
      if retries > 0
        amt = a * base ** tries
        @log.debug "Operation failed; sleeping #{amt}s then retrying" if @log
        sleep(amt)
        retries -= 1
        retry
      else
        raise e
      end
    end
    return result
  end


  # Retries a block that returns nil, false, or integer < 1 upon failure,
  # using a timed backoff. If all retries fail, the last error code is returned.
  # If the block succeeds, the return value of the block is returned, so this
  # method is safe to use as the rhs of an assignment.
  # @param [Integer] retries The number of times to retry the block.
  # @param [Integer,Float] first_delay Number of seconds to delay between
  #                                       the first failure and the first retry.
  # @param [Integer,Float] base The base to use when calculating successive
  #                             delays according to the formula
  #                             delay = first_delay/base * (base ** num_tries),
  #                             where num_tries increments monotonically from 1.
  #                             The default base is Math.exp(1); that is, 'e',
  #                             so that the default backoff is exponential. To
  #                             use a constant delay between retries, set base
  #                             equal to 1.
  def EZQ.boolean_retry_with_backoff(retries,
                                      first_delay=1,
                                      base=Math.exp(1),
                                      &block)
    a = Float(first_delay) / Float(base)
    tries = 0
    result = false
    while !result or result < 1
      tries += 1
      result = block.call()
      if (!result or result < 1) and retries > 0
        amt = a * base ** tries
        @log.debug "Operation failed; sleeping #{amt}s then retrying" if @log
        sleep(amt)
        retries -= 1
        next
      else
        break
      end
    end
    return result
  end


  # Runs an external command.
  # @param [String,Array] cmd The command to run. If +cmd+ is a single string,
  #        it will be run via the default shell. If +cmd+ is an array with two
  #        or more terms, the command will be run without passing through a
  #        shell, with the first array entry referring to the program name, and
  #        all other entries referring to commandline arguents to the program.
  #        The array form is particularly useful for passing long strings to
  #        an external program without escape the string(S) to accomodate
  #        the shell.
  # @return Returns an array containing a success flag in the
  # first position, and an array of strings containing all stdout and stderr
  # output in the second position. The success flag can be true, false, or nil.
  # True indicates the external command ran and exited with exit_status = 0.
  # False indicates the command ran and exited with exist_status != 0.
  # Nil indicates an exception was raised in Ruby when attempting to run the
  # command. In this case, the output array in the second position will
  # contain the text of the exception.
  def EZQ.exec_cmd(cmd)
    success = false
    output = []
    begin
      IO.popen(cmd,:err=>[:child, :out]) do |io|
        while !io.eof?
          output << io.gets
        end
        io.close
        success =  $?.to_i.zero?
      end
    rescue => e
      success = nil # mimic behavior of Kernel#system
      output << e
    end
    return [success,output]
  end

end
