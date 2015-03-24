require 'logger'

# Logger that logs messages to nothing. The intent of this logger is
# to degrade application performance in a way similar to a real logger.
# Unlike a typical NullLogger which overrides all of the logging
# methods with a no-op, this one will perform all the costly message
# formation, formatting, and routing; the file descriptor at the very
# end of the chain just points off into nothingness.
class DevNullLog < Logger

  def initialize
    log_file = RUBY_PLATFORM =~ /mswin|mingw/ ? 'NUL:' : '/dev/null'
    super(log_file)
  end

end
