require 'ezq/utils/common'


module EZQ

  # Retries a block that raises exceptions, using a timed backoff. If all
  # retries fail, the last raised exception is raised by this function. If
  # the block succeeds, the return value of the block is returned, so this
  # method is safe to use as the rhs of an assignment.
  #
  # @param [Integer] retries The number of times to retry the block.
  #
  # @param [Integer,Float] first_delay Number of seconds to delay between
  #   the first failure and the first retry.
  #
  # @param [Integer,Float] base The base to use when calculating successive
  #   delays according to the formula delay = first_delay/base * (base **
  #   num_tries), where num_tries increments monotonically from 1. The default
  #   base is Math.exp(1); that is, 'e', so that the default backoff is
  #   exponential. To use a constant delay between retries, set base equal to
  #   1.
  #
  # @param [Integer,Float] jitter Jitter successive retries by up to `jitter`
  #   seconds.
  #
  # @return [?] The result of the provided block
  def EZQ.exceptional_retry_with_backoff(retries,
                                          first_delay=1,
                                          base=Math.exp(1),
                                          jitter=0,
                                          &block)
    a = Float(first_delay) / Float(base)
    tries = 0
    result = nil
    begin
      tries += 1
      result = block.call()
    rescue => e
      if retries > 0
        # Convert jitter to float on interval [0, jitter) with resolution of 10ms
        jit = jitter > 0 ? rand(jitter*100) / 100.0 : 0
        amt = a * base ** tries + jit
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
  #
  # @param [Integer] retries The number of times to retry the block.
  #
  # @param [Integer,Float] first_delay Number of seconds to delay between
  #   the first failure and the first retry.
  #
  # @param [Integer,Float] base The base to use when calculating successive
  #   delays according to the formula delay = first_delay/base * (base **
  #   num_tries), where num_tries increments monotonically from 1. The default
  #   base is Math.exp(1); that is, 'e', so that the default backoff is
  #   exponential. To use a constant delay between retries, set base equal to
  #   1.
  #
  # @param [Integer,Float] jitter Jitter successive retries by up to `jitter`
  #   seconds.
  #
  # @return [?] The result of the provided block
  def EZQ.boolean_retry_with_backoff(retries,
                                      first_delay=1,
                                      base=Math.exp(1),
                                      jitter=0,
                                      &block)
    a = Float(first_delay) / Float(base)
    tries = 0
    result = false
    while !result or (result.kind_of?(Numeric) && (result < 1))
      tries += 1
      result = block.call()
      if (!result or (result.kind_of?(Numeric) && (result < 1))) and retries > 0
        # Convert jitter to float on interval [0, jitter) with resolution of 10ms
        jit = jitter > 0 ? rand(jitter*100) / 100.0 : 0
        amt = a * base ** tries + jit
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

end
