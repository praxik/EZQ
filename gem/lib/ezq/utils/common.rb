

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

  # Sets the logger for ezqlib calls. Pass in something that reponds like
  # Ruby's builtin Logger class and you're good to go.
  # @param [Logger-like] logger The Logger to use. Must respond to Logger
  #   methods and Logger level constants, but need not be derived from Logger.
  def EZQ.set_logger(logger)
    @log = logger
  end


  # Returns a new array of size +size+ which contains the elements of +ary+
  # repeated cyclically.
  def EZQ.cyclical_fill( ary,size )
    elem = ary.cycle
    result = []
    size.times{result << elem.next}
    return result
  end

end
