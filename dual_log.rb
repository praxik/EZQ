#!usr/bin/env ruby

# Example use of the DualLogger class
#d = DualLogger.new({:progname=>"my_app_name",
#                    :ip=>"my_ip",
#                    :filename=>"dl.log",
#                    :local_level=>Logger::INFO,
#                    :loggly_token=>"loggly-token-goes-here",
#                    :loggly_level=>Logger::ERROR,
#                    :pid=>Process.pid})

require 'json'
require 'logglier'
require_relative 'multi_logger.rb'


# This isn't the class you want. This is just a wrapper around Logglier. You
# want the class that comes after this one. Really. I'm serious. Why are you
# still reading this?
class LogglyWrapper < Logger

 
  def initialize(opts={})

    @progname = opts.fetch(:progname,'not_set')
    @ip = opts.fetch(:ip,'not_set')
    @pid = opts.fetch(:pid,0)

    trash_log = File.open(File::NULL, "w")

    loggly_token = opts.fetch(:loggly_token,'')
    if loggly_token and !loggly_token.empty?
      @loggly = Logglier.new("http://logs-01.loggly.com/inputs/#{loggly_token}/tag/#{@progname}/",:format => :json,:threaded => true,:failsafe=> trash_log)
      @loggly.level = opts.fetch(:loggly_level,Logger::ERROR)
    else
      return nil
    end
  end

  # Takes a message and wraps all the metadata we want to carry with it
  def add_metadata(msg)
    return {:timestamp=>Time.now.utc.strftime('%FT%T.%3NZ'),:msg=>msg,:ip=>@ip,:pid=>@pid}
  end

  def datetime_format=(datetime_format)
    @loggly.datetime_format = datetime_format
  end

  def level=(level)
    @loggly.level = level
  end

  def level
    return @loggly.level
  end

  def progname=(progname)
    @loggly.progname=progname
  end

  def progname
    return @loggly.progname
  end

  # Methods that write to logs just write to each contained logger in turn
  def add(severity, message = nil, progname = nil, &block)
    @loggly.add(severity, add_metadata(message), progname, &block)
  end
  alias log add

  def <<(msg)
    @loggly << add_metadata(msg)
  end

  def debug(progname = nil, &block)
    @loggly.debug(add_metadata(progname), &block)
  end

  def info(progname = nil, &block)
    @loggly.info(add_metadata(progname), &block)
  end

  def warn(progname = nil, &block)
    @loggly.warn(add_metadata(progname), &block)
  end

  def error(progname = nil, &block)
    @loggly.error(add_metadata(progname), &block)
  end

  def fatal(progname = nil, &block)
    @loggly.fatal(add_metadata(progname), &block)
  end

  def unknown(progname = nil, &block)
    @loggly.unknown(add_metadata(progname), &block)
  end

end


# DualLogger works a lot like Ruby's built-in Logger class -- because it uses
# it under the hood -- but makes it very easy to both log to a local file AND
# send JSON log messages straight to Logggly. The constructor is the main
# thing that's different from using Logger. Otherwise, you use it in
# exactly the same way. Log messages are duplexed to the local file as well as
# Loggly, with the Loggly ones being sent on a separate, dedicated sender
# thread.
class DualLogger < MultiLogger

  # Options hash consists of the following, all of which are optional -- but you
  # really ought to set either :filename or :loggly_token, otherwise this logger
  # doesn't do anything. To get the most out of this logger, you should really
  # set all of them.
  # :progname -- name of the application being logged. If using Loggly, setting
  #              this correctly will make your life soooo much easier.
  # :filename -- name of a local file to use for local logging. If unset, no
  #              local logging occurs
  # :local_level -- level for local file log. Use the Logger:: level constants.
  #                 Defaults to Logger::INFO if unset
  # :loggly_token -- Token to Loggly service. No logging to Loggly will occur
  #                  if this option is not specified
  # :loggly_level -- level to use for Loggly service. Use the Logger:: level
  #                  constants. Defaults to Logger::ERROR. Notice the default
  #                  is different from the local logger default.
  # :ip -- ip address of the instance. This is only used for Loggly logs
  # :pid -- process id of the calling application. You can easily supply this
  #         via Process.pid. Used only for Loggly logs
  def initialize(opts={})

    @progname = opts.fetch(:progname,'not_set')
    
    filename = opts.fetch(:filename,nil)
    local = nil
    if filename
      local = Logger.new(filename)
      local.level = opts.fetch(:local_level,Logger::INFO)
    end

    loggly = LogglyWrapper.new(opts) if opts.fetch(:loggly_token,false)

    logs = []
    logs << local if local
    logs << loggly if loggly

    super(logs)
    # This is the exact timestamp format loggly requires
    self.datetime_format= '%FT%T.%3NZ'
    self.progname = @progname
  end

  def level=(level)
    # no-op. We do not want to allow the user to unintentionally synchronize
    # the level for all the backend loggers.
  end

end
