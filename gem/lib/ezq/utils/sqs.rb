require 'aws-sdk'
require 'securerandom'
require 'digest/md5'
require 'yaml'

require 'ezq/utils/common'
require 'ezq/utils/s3'

# TODO: Add a polling method to this, so that explicit Aws api use can be removed from
# Processor and from the Nimbus apps


module EZQ


  def EZQ.set_message_visibility(queue_url, msg, timeout)
    Aws::SQS::Client.new.change_message_visibility(
        queue_url: queue_url, receipt_handle: msg[:receipt_handle],
        visibility_timeout: timeout)
    return nil
  rescue
    @log.warn "Failed to reset timeout on msg #{msg[:message_id]}"
  end


  def EZQ.delete_queue_message(queue_url, msg)
    Aws::SQS::Client.new.delete_message(queue_url: queue_url, receipt_handle: msg[:receipt_handle])
    return nil
  rescue
    @log.warn "Failed to delete msg #{msg[:message_id]}"
  end


  def EZQ.get_queue_timeout(queue_url)
    return Aws::SQS::Client.new.get_queue_attributes(queue_url: queue_url,
          attribute_names: ['VisibilityTimeout'])
  end


  # Diverts the body of a message intended for SQS off to S3. This is useful
  # when the message size exceeds SQS's limit.
  #
  # @param [String] body The message body
  #
  # @param [Hash] preamble The existing EZQ preamble has for the message
  #
  # @param [String] bucket_name S3 bucket to use for overflow.
  #   Defaults to 'Overflow'.
  #
  # @param [String] key The S3 key to use. Defaults to a random uuid.
  #
  # @return [Array(String,String)] Returns the new body and new preamble that
  #   should be sent on to a queue in place of the original.
  def EZQ.divert_body_to_s3( body,
                              preamble,
                              bucket_name = 'Overflow',
                              key = SecureRandom.uuid() )
    @log.debug "EZQ::divert_body_to_s3" if @log
    bucket_name = 'EZQ.Overflow' if bucket_name == nil
    key = SecureRandom.uuid if key == nil
    new_preamble = preamble.clone
    EZQ.send_data_to_s3( body,bucket_name,key )
    new_preamble['EZQ'] = {} if !new_preamble.has_key?('EZQ') or new_preamble['EZQ'] == nil
    new_preamble['EZQ']['get_s3_file_as_body'] = {'bucket'=>bucket_name,'key'=>key}
    body = "Message body was diverted to S3 as s3://#{bucket_name}/#{key}"
    return [body,new_preamble]
  end


  def EZQ.create_queue(name)
    sqs = Aws::SQS::Client.new()
    return sqs.create_queue(queue_name: name)
  end


  def EZQ.delete_queue(name)
    sqs = Aws::SQS::Client.new()
    qurl = queue =~ /^http:|^https:/ ? name : sqs.get_queue_url(queue_name: name).queue_url
    sqs.delete_queue(queue_url: qurl)
    return nil
  end


  # Enqueues an array of messages in SQS, automatically diverting each body to
  # S3 if it is larger than SQS allows.
  #
  # @param [String,Array] body_ary Array of message bodies
  #
  # @param [Hash,Array] preamble_ary Array of EZQ preambles to use for the messages.
  #   If this array is smaller than body_ary, the elements in it will be
  #   repeated cyclically to match up with body_ary.
  #
  # @param [AWS::SQS::Queue,String] queue URL or name of an SQS
  #   queue. If a URL, the queue *must* already exist
  #
  # @param [Bool] create_queue_if_needed Create the queue if it doesn't exist?
  #
  # @param [String] bucket Name of S3 bucket to use for overflow if
  #   message is too large for SQS
  #
  # @param [String,Array] key_ary Array of S3 keys to use if message is too
  #   large for SQS.
  #
  # @return [String,Array] Array of MD5 hexdigests of the sent messages. The
  #   strings will be empty if the messages failed to send.
  def EZQ.enqueue_batch( body_ary,
                          preamble_ary,
                          queue,
                          create_queue_if_needed = false,
                          bucket = nil,
                          key_ary = [] )
    @log.debug "EZQ::enqueue_batch" if @log
    #q = get_queue(queue,create_queue_if_needed)

    sqs = Aws::SQS::Client.new()

    if queue =~ /^http:|^https:/ # Queue "name" is actually a url
      qurl = queue
    else
      begin
        qurl = sqs.get_queue_url(queue_name: queue).queue_url
      rescue Aws::Errors::QueueDoesNotExist
        if create_queue_if_needed
          qurl = EZQ.create_queue(queue).queue_url
        else
          return ['']*body_ary.size
        end
      end
    end

    # Just to be safe; should never happen in practice.
    #return ['']*body_ary.size if !q.respond_to?( :send_message )

    preambles = cyclical_fill( Array(preamble_ary),body_ary.size )
    keys = fill_array( key_ary,body_ary.size,nil )
    msgs = body_ary.zip( preambles ).zip( keys ).flatten.
      map_slice(3){|body,preamble,key| prepare_message_for_queue( body,preamble,bucket,key )}

    digests = msgs.map{|msg| Digest::MD5.hexdigest( msg )}


    mdfives = []
    #msgs.each_slice(10){|batch| mdfives += q.batch_send( *batch ).map{|sent| sent.md5}}

    msgs.map.with_index{|msg,idx| {message_body: msg, id: idx.to_s}}.
      each_slice(10) do |es|
        mdfives += sqs.send_message_batch(queue_url: qurl, entries: es).
                    successful.
                    sort_by!{|el| el[:id]}.
                    map{|r| r[:md5_of_message_body]}
      end

    return digests if digests == mdfives
    # If any one message digest doesn't match, behave as though the whole
    # batch failed (because it probably did!)
    @log.warn "EZQ::enqueue_batch: digests didn't match" if @log
    return ['']*body_ary.size
  end



  # Enqueues a message in SQS, automatically diverting the body to S3
  # if it is larger than SQS allows.
  #
  # @param [String] body The message body
  #
  # @param [Hash] preamble The EZQ preamble to use for this message
  #
  # @param [String] queue Name of the SQS queue
  #
  # @param [Bool] create_queue_if_needed Create the queue if it doesn't exist?
  #
  # @param [String] bucket Name of S3 bucket to use for overflow if
  #   message is too large for SQS
  #
  # @param [String] key S3 key to use if message is too large for SQS
  #
  # @return [String] MD5 hexdigest of the sent message. The return string will
  #   be empty if the message failed to send.
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
  #
  # @param [Array] ary Input array from which to draw elements
  #
  # @param [Integer] size  Size of array to return
  #
  # @param [Object] filler The fill object to use to pad the end of the array
  #   if +ary+ is smaller than +size+.
  #
  # @return [Array] an array containing up to +size+ elements of +ary+.
  #   If +ary+ is smaller than +size+, the remainder of the returned array
  #   contains +filler+.
  def EZQ.fill_array( ary,size,filler )
    if ary.size < size
      return ary + Array.new( size-ary.size,filler )
    else
      return ary.take( size )
    end
  end



  # Returns the desired queue, creating it if necessary and requested. Returns
  # nil if the queue could not be created.
  #
  # @param [AWS::SQS::Queue,String] queue SQS::Queue object or name of queue
  #
  # @param [Bool] create_if_needed Create the queue if it doesn't exist?
  #
  # @return [AWS::Queue] Returns the requested queue, or nil.
  #
  # @raise Raises exception if the queue could not be created when it didn't
  #   already exist and create_if_needed was true.
#   def EZQ.get_queue( queue,create_if_needed=false )
#     @log.debug "EZQ::get_queue" if @log
#     # If it's an SQS::Queue already, treat it as such....
#     if queue.respond_to?( :send_message )
#       begin
#         return queue if queue.exists?
#       rescue(AWS::SQS::Errors::ExpiredToken)
#         return EZQ.get_queue( queue.name, create_if_needed)
#       end
#       return AWS::SQS.new.queues.create(queue.name) if create_if_needed
#       return nil
#     end

#     # Nope, it's a string.
#     queue = queue.strip
#     sqs = AWS::SQS.new
#     begin
#       return sqs.queues.named( queue )
#     rescue
#       return sqs.queues.create( queue ) if create_if_needed
#       return nil
#     end
#   end



  # Transforms a separate body and preamble into a single message suitable for
  # SQS. If the body + preamble is too big to fit in SQS, this method will
  # automatically divert the body to S3 and return a message containig a new
  # (smaller) body with a mutated preamble.
  #
  # @param [String] body Body for the message
  #
  # @param [Hash] preamble The EZQ preamble hash for this message
  #
  # @param [String] bucket Name of S3 bucket to use for message that are too
  #   large for SQS.
  #
  # @param [String] key S3 key name to use if message is too large for SQS
  #
  # @return [String] The full message with the preamble and body concatenated
  #   together. Both will have been altered if the body had to be diverted to
  #   S3.
  def EZQ.prepare_message_for_queue( body,preamble,bucket=nil,key=nil )
    @log.debug "EZQ::prepare_message_for_queue" if @log
    # 256k limit minus assumed metadata size of 6k:  (256-6)*1024 = 256000
    bc = body.clone
    if (body.bytesize + preamble.to_yaml.bytesize) > 256000
      bc,preamble = EZQ.divert_body_to_s3( bc,preamble,bucket,key )
    end

    return bc.insert( 0,"#{preamble.to_yaml}...\n" )
  end

end
