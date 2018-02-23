require 'aws-sdk'
# These strange ox includes are to deal with a namespacing issue on Windows
# that is most likely not worth the trouble to fully track down.
require 'ox/node'
require 'ox/raw'

# Ensure sane certs on used on Windows
Aws.use_bundled_cert! if RUBY_PLATFORM =~ /mswin|mingw/

# Not sure I want to monkey-patch like this. It might be better to have
# a function rather than a method.

module Enumerable
  # Maps block onto each slice of +num+ elements in the enumerable.
  # This method cribbed from
  # http://rubydoc.info/github/svenfuchs/space/Enumerable:map_slice
  #
  # @param [Integer] num Number of elements to include in each slice
  def map_slice(num, &block)
    result = []
    each_slice(num) { |element| result << yield(element) }
    return result
  end
end



module EZQ

  # Sets the logger for ezqlib calls. Pass in something that reponds like
  # Ruby's builtin Logger class and you're good to go.
  #
  # @param [Logger-like] logger The Logger to use. Must respond to Logger
  #   methods and Logger level constants, but need not be derived from Logger.
  def EZQ.set_logger(logger)
    @log = logger
  end


  # Returns a new array of size +size+ which contains the elements of +ary+
  # repeated cyclically.
  #
  # @param [Array] ary The source array
  #
  # @param [Integer] size The size of the new Array to return
  #
  # @return [Array] An array of size +size+
  def EZQ.cyclical_fill( ary,size )
    elem = ary.cycle
    result = []
    size.times{result << elem.next}
    return result
  end

end
