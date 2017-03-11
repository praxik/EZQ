require 'aws-sdk'
require 'digest/md5'
require 'fileutils'
require 'mimemagic'

require 'ezq/utils/common'
require 'ezq/utils/retry'
require 'ezq/utils/compression'

# TODO: refactor most of this into service-agnostic calls
# ...actually, this file should stay as-is, and there should be a higher
# level interface above this one that abstracts everything as indicated below.
#
# EZQ.to_remote(:service,:opts_for_service)
# EZQ.from_remote(:service,:opts_for_service)

# module EZQ

#   # service is a symbol; one of :S3, :URL
#   # remote_info is a service-specific hash
#   def EZQ.to_remote(local_filename, service, remote_info, opts={})
#   end

#   def EZQ.from_remote(service, remote_info, opts={}, local_filename)
#   end

# end

module EZQ

  # Returns S3 Client object.
  #
  # @param [Bool] reset If true, get a new S3 Client object rather than
  #   returning the cached one
  #
  # @return [AWS::S3] the S3 Client object
#   def EZQ.get_s3(reset: false)
#     @s3 = nil if reset
#     # The backoff retry is an attempt to deal with this uninformative error:
#     # `block in add_service': undefined method `global_endpoint?' for AWS::S3:
#     #  Class (NoMethodError)
#     @s3 ||= exceptional_retry_with_backoff(5,1,1){Aws::S3::Client.new()}#AWS::S3.new()}

#     while !@s3.respond_to?(:get_object)
#       @log.warn "Aws::S3 isn't responding to ':get_object'." if @log
#       @s3 = exceptional_retry_with_backoff(5,1,1){Aws::S3::Client.new()}
#     end
#     return @s3
#   end

  # Region is usually pulled from environment. Can be overridden for special
  # requests.
  def EZQ.s3_resource(region: nil)
    if region.nil?
      @s3_res ||= Aws::S3::Resource.new()
      return @s3_res
    else
      # No memo-ization, since specific region request should be non-standard!
      return Aws::S3::Resource.new(region: region)
    end
  end



  # Sends data to S3 using bucket_name and key
  #
  # @param [String] data Data to send
  #
  # @param [String] bucket_name S3 Bucket. If the bucket doesn't already exist,
  #   it will be automatically created.
  #
  # @param [String] key S3 key to use.
  #
  # @param [Bool] compress If true, compresses data with gzip and alters key
  #   to end with '.gz'. Default: false. Note this is a *named* *parameter*.
  #
  # @return [String] returns the key that was sent to S3. Note that if
  #   +compress+ was true, the key may have been mutated from what you
  #   passed in.
  def EZQ.send_data_to_s3( data,bucket_name,key,compress: false )
    thread = send_data_to_s3_async( data,bucket_name,key,compress: compress )
    thread.join
    return thread.value
  end



  # Sends data to S3 on a new thread, using bucket_name and key
  #
  # @param [String] data Data to send
  #
  # @param [String] bucket S3 Bucket. If the bucket doesn't already exist,
  #   it will be automatically created.
  #
  # @param [String] key S3 key to use.
  #
  # @param [Bool] compress If true, compresses data with gzip and alters key
  # to end with '.gz'. Default: false. Note this is a *named* *parameter*.
  #
  # @return [Thread] Returns a handle to a Thread. You should ensure that
  #   Thread#join is called on this thread before exiting your
  #   application. Once the thread has joined, you can call Thread#value to
  #   get the actual key name that was pushed. Note that if +compress+ was
  #   true, the key may have been mutated from what you passed in.
  def EZQ.send_data_to_s3_async( data,bucket,key,compress: false )
    return Thread.new(data,bucket,key,compress){ |d,b,k,c| DataPusher.new.push(d,b,k,c,@log) }
  end



  # Sends a file to S3, using bucket_name and key
  #
  # @param [String] filename Path of file to send
  #
  # @param [String] bucket S3 Bucket. If the bucket doesn't already exist,
  #   it will be automatically created.
  #
  # @param [String] key S3 key to use.
  #
  # @param [Hash] options An S3 object options hash.
  #   See docs for AWS::S3::S3Object#write
  #
  # @param [Bool] compress If true, compresses file with gzip and alters key
  #  to end with '.gz'. Default: false. Note this is a *named* *parameter*.
  #
  # @return [String] returns the key that was sent to S3. Note that if
  #   +compress+ was true, the key may have been mutated from what you
  #   passed in.
  def EZQ.send_file_to_s3( filename,bucket,key,options: {},compress: false )
    thread = send_file_to_s3_async( filename,bucket,key,options: options,compress: compress )
    thread.join
    return thread.value
  end



  # Sends a file to S3 on a new thread, using bucket_name and key
  #
  # @param [String] filename Path of file to send
  #
  # @param [String] bucket S3 Bucket. If the bucket doesn't already exist,
  #   it will be automatically created.
  #
  # @param [String] key S3 key to use.
  #
  # @param [Hash] options An S3 object options hash.
  #   See docs for AWS::S3::S3Object#write
  #
  # @param [Bool] compress If true, compresses file with gzip and alters key
  #   to end with '.gz'. Default: false. Note this is a *named* *parameter*.
  #
  # @return [Thread] Returns a handle to a Thread. You should ensure that
  #   Thread#join is called on this thread before exiting your
  #   application. Once the thread has joined, you can call Thread#value to
  #   get the actual key name that was pushed. Note that if +compress+ was
  #   true, the key may have been mutated from what you passed in.
  def EZQ.send_file_to_s3_async( filename,bucket,key,options: {},compress: false )
    return Thread.new(filename,bucket,key,options,compress){ |f,b,k,o,c| FilePusher.new.push(f,b,k,o,c,@log) }
  end



  # Sends a file to S3, using the filename as the key.
  #
  # @param [String] bucket_comma_filename Bucket and filename (key) joined with
  #   a comma and no spaces. The bucket will be created if it doesn't exist.
  #
  # @param [Hash] options An S3 object options hash.
  #   See docs for AWS::S3::S3Object#write
  #
  # @param [Bool] compress If true, compresses file with gzip and alters key
  #   to end with '.gz'. Default: false. Note this is a *named* *parameter*.
  #
  # @return [nil] Always returns nil.
  def EZQ.send_bcf_to_s3( bucket_comma_filename,options: {},compress: false )
    thread = send_bcf_to_s3_async( bucket_comma_filename,options: options,compress: compress )
    thread.join
    return thread.value
  end



  # Sends a file to S3, using the filename as the key.
  #
  # @param [String] bucket_comma_filename Bucket and filename (key) joined with
  #   a comma and no spaces. The bucket will be created if it doesn't exist.
  #
  # @param [Hash] options An S3 object options hash.
  #   See docs for AWS::S3::S3Object#write
  #
  # @param [Bool] compress If true, compresses file with gzip and alters key
  #   to end with '.gz'. Default: false. Note this is a *named* *parameter*.
  #
  # @return [Thread] Returns a handle to a Thread. You should ensure that
  #   Thread#join is called on this thread before exiting your application.
  def EZQ.send_bcf_to_s3_async( bucket_comma_filename, options: {},compress: false )
    b_k_ary = bucket_comma_filename.split(',').map{|s| s.strip}
    bucket = b_k_ary.shift
    key = b_k_ary.join(',')
    return send_file_to_s3_async( key,bucket,key,options: options,compress: compress )
  end



  # DataPusher class pushes specified data to S3. It is intended to be used
  # as a thread object.
  class DataPusher
    def push( data,bucket_name,key,compress=false,log=nil )
      @retries ||= 10
      options = {:content_type => EZQ.get_content_type(key)}

      if compress
        sio = StringIO.new
        gz = Zlib::GzipWriter.new(sio,9)
        gz.write(data)
        gz.close
        data = sio.string
        key = "#{key}.gz" if !(key =~ /\.gz$/)
        options[:content_encoding] = 'gzip'
      end

      s3 = EZQ.s3_resource()
      s3.create_bucket( {bucket: bucket_name} ) if !s3.bucket(bucket_name).exists?
      obj = s3.bucket(bucket_name).object(key)
      dig = Digest::MD5.hexdigest(data)
      if obj.exists? and ((obj.etag() == dig) or (obj.metadata.fetch('md5','') == dig ))
        log.debug "Remote file is up-to-date; skipping send." if log
        return key
      end
      md5_opts = {:metadata=>{:md5=>dig}}
      all_opts = options.merge(md5_opts)
      all_opts[:body] = data
      obj.put(all_opts)
      return key
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

    # Push a file into Amazon S3 storage, setting the content_type along the
    # way, and compressing with gz (and setting content_encoding if so) if
    # requested. Content_type is *not* overwritten if you have already passed
    # a +:content_type+ in the options hash.
    #
    # @param [String] filename Path to local file to send to S3
    #
    # @param [String] bucket_name Name of S3 bucket to use as endpoint.
    #   We attempt to create bucket if it does not already exist.
    #
    # @param [String] key Key to use for file in S3. This is the S3 equivalent
    #   of a full path to the file. Note that +key+ need not be the same
    #   as +filename+. If +filename+ does not end in +".gz"+ but +key+ does,
    #   the file will be compressed with gzip *even if the* +compress+
    #   *parameter is false*.
    #
    # @param [Hash] options An S3 object options hash.
    #   See docs for AWS::S3::S3Object#write
    #
    # @param [Bool] compress Whether to compress the file before sending.
    #   If +true+, and +key+ does not end in +".gz"+, +key+ will be changed
    #   to end in +".gz"+.
    #
    # @param [Logger] log Logger to use for loggish purposes.
    #
    # @return [String] The key that was sent to S3. Notice the key might be
    #   different from the one passed in if +compress+ was +true+.
    def push( filename,bucket_name,key,options={},compress=false,log=nil )
      @retries ||= 10
      @log = log
      raise_if_no_file(filename)
      set_content_type(filename,options) # mutates options
      local_file, key2, compress2 = handle_compression(filename,key,options,compress) # mutates options
      set_md5(local_file,options) # mutates options

      @log.debug "Sending #{filename} to S3://#{bucket_name}/#{key2}" if @log

      begin
        obj = get_or_create_s3_obj(bucket_name,key2)
        if !up_to_date?(obj,options)
          obj.upload_file(local_file,options)
        end
      rescue => e
        @log.warn "EZQ::FilePusher: #{e}" if @log
        retry if (@retries -= 1) > -1
        @log.error "EZQ::FilePusher: #{e}" if @log
        raise e
      ensure
        # Cleanup the temporary compressed file
        File.unlink(local_file) if compress2 && File.exist?(local_file)
        return key2
      end
    end

    protected # Everything in this class below this line is protected
    def raise_if_no_file(filename)
      if !File.exists?(filename)
        raise "File '#{filename}' does not exist."
      end
    end

    def handle_compression(local_file,key,options,compress)
      # If filename doesn't end in .gz but key does, we will compress
      # *even if* +compress+ *is false*
      gz = /\.gz$/
      silent_compress = ( !(local_file =~ gz) && key =~ gz )
      @log.debug "EZQ::FilePusher: implicitly gzipping #{local_file}" if (@log && silent_compress)
      compress |= silent_compress
      if compress
        local_file = EZQ.compress_file(local_file)
        options[:content_encoding] = 'gzip'
        key = "#{key}.gz" if !(key =~ gz)
      end
      return [local_file,key,compress]
    end

    def set_content_type(filename,options)
      if !options.fetch(:content_type,false)
        options[:content_type] = EZQ.get_content_type(filename)
      end
    end

    def set_md5(local_file,options)
      options[:metadata] = {:md5=>EZQ.md5file(local_file).hexdigest}
    end

    def up_to_date?(obj,options)
      utd = false
      file_dig = options[:metadata][:md5]
      if obj.exists? and ((obj.etag() == file_dig) or (obj.metadata.to_h.fetch('md5','') == file_dig ))
        @log.debug "Remote file is up-to-date; skipping send." if @log
        utd = true
      end
      return utd
    end

    def get_or_create_s3_obj(bucket_name,key)
      s3 = EZQ.s3_resource()
      s3.create_bucket( {bucket: bucket_name} ) if !s3.bucket(bucket_name).exists?
      return s3.bucket(bucket_name).object(key)
    end

  end



  # Pulls a single file down from S3. This method checks for the existence of
  # a local file with the same name as the one in S3. If the local file
  # exists and its md5 is the same as the file in S3, the method returns true
  # without actually downloading the file.
  #
  # @param [String] bucket The S3 bucket from which to pull
  #
  # @param [String] key The S3 key, which will also map directly to the local
  #   filename
  #
  # @param [Bool] decompress Whether to decompress the file (.gz or .zip).
  #   Default: false. Note this is a *named* *parameter*.
  #
  # @param [Bool] keep_name If true and +decompress+ is true, decompressed file
  #   will have the same name as compressed file, including .gz or .zip
  #   extension. Note this is a *named* *parameter*.
  #
  # @return [Bool] true if successful, false otherwise
  def EZQ.get_s3_file(bucket,key,decompress: false, keep_name: false, region: nil, response_target: nil)
    @log.debug "EZQ::get_s3_file '#{bucket}/#{key}'" if @log
    obj = EZQ.s3_resource(region: region).bucket(bucket).object(key)
    key = response_target unless response_target.nil?

    # Do we already have a current version of this file?
    if File.exists?(key)
      dig = EZQ.md5file(key).hexdigest
      if ((obj.etag().gsub("\"","") == dig) || (obj.metadata.to_h.fetch('md5','').gsub("\"","") == dig ))
        @log.debug "EZQ::get_s3_file: local file is already current" if @log
        return true
      end
    end

    FileUtils.mkdir_p(File.dirname(key))
    resp = obj.get(response_target: key)

    already_decompressed = false
    if (resp.content_encoding == "gzip") && (resp.content_type != "application/x-gzip")
      # .gz file ext means leave compressed even if content_type wasn't set
      if File.extname(key) != ".gz"
        EZQ.gunzip(key, keep_name: true)
        already_decompressed = true
      end
    end

    if decompress && !already_decompressed
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
  rescue => e
      @log.error "EZQ::get_s3_file: #{e}" if @log
      return false
  end


  # Removes a file from S3. This will only remove individual files,
  # not complete "directories"
  #
  # @param [String] bucket S3 bucket in which the file is stored
  #
  # @param [String] key S3 key to the file
  #
  # @return nil
  def EZQ.remove_s3_file(bucket,key)
    @log.debug "EZQ::remove_s3_file: #{bucket}/#{key}" if @log
    s3 = EZQ.s3_resource()
    s3.bucket(bucket).object(key).delete()
    return nil
  end


  # Get md5 of a file without having to read entire file into memory at once.
  # From https://www.ruby-forum.com/topic/58563
  #
  # @param [String] filename The name of the file to digest.
  #
  # @return [Digest] An md5 digest object.
  def EZQ.md5file(filename)
    return Digest::MD5.new unless File.exist?(filename)
    md5 = File.open(filename, 'rb') do |io|
      dig = Digest::MD5.new
      buf = ""
      dig.update(buf) while io.read(4096, buf)
      dig
    end
    return md5
  end


  # Get the content/mime type of a file.
  #
  # @param [String] path Path to file.
  #
  # @return [String] the content type
  def EZQ.get_content_type(path)
    # MimeMagic gem currently doesn't recognize json
    return 'application/json' if File.extname(path) == '.json'

    mag = nil
    if File.exist?(path)
      mag = MimeMagic.by_magic(File.open(path))
      mag = if mag then mag else MimeMagic.by_path(path) end
    else
      mag = MimeMagic.by_path(path)
    end
    res = if mag then mag.type else '' end
    return res
  end

end
