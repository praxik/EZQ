
require 'aws/sqs/queue'
require 'ezq/utils/retry'

# We have to monkey-patch the AWS module in order to derive from Queue because
# AWS does some internal magic with namespaces. Perhaps there is a cleaner way,
# but this is fairly safe because the monkey-patching only adds a method, and
# does so by subclassing Queue. We are, of course, also adding a new internal
# class to SQS, but there's just no way around that.
module AWS
  class SQS

    # Provides a polling operation that does not automatically delete a message
    # at the end of the block. This is useful when an application needs to have
    # the flexibility to fail on processing a message and move on to the next
    # message in the queue without deleting the failed one from the queue.
    class X_queue < Queue

      # Similar to AWS::SQS::Queue#poll.
      #
      # This version takes all the same options, but does not automatically
      # delete the message from the queue at the end of the block. Instead,
      # user code in the block must explicitly delete the message.
      def poll_no_delete(opts = {}, &block)
            opts[:limit] = opts.delete(:batch_size) if
              opts.key?(:batch_size)

            opts[:wait_time_seconds] = DEFAULT_WAIT_TIME_SECONDS unless
              opts.has_key?(:wait_time_seconds)

            last_message_at = Time.now
            got_first = false
            loop do
              got_msg = false
              message = EZQ.exceptional_retry_with_backoff(3){receive_messages(opts)}
              if message && !(message.is_a?(Array) && message.empty?)
                got_msg = got_first = true
                last_message_at = Time.now
                yield(message)
              end
              unless got_msg
                return if hit_timeout?(got_first, last_message_at, opts)
              end
            end
            nil
          end
    end

  end
end
