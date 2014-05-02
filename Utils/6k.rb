#!/usr/bin/env ruby

# Unified management script for 6k workflow

# Run this as
# ./6k --help
# to get a description of the options.

argv = ARGV
# uncomment and fill out to debug in an instaREPL
argv = ['list','all']
pwd = "#{File.expand_path(File.dirname(__FILE__))}"

require 'bundler/setup'
require 'yaml'
require 'aws-sdk'
require 'optparse'
#require './SixK.rb'
require "#{pwd}/SixK.rb"


@creds_file = "#{pwd}/credentials.yml" # credentials file
config = "#{pwd}/nimbus_config.yml"

def setup_AWS
  if !File.exists?(@creds_file)
    warn "Credentials file '#{@creds_file}' does not exist! Aborting."
    exit 1
  end

  credentials = YAML.load(File.read(@creds_file))
  if !credentials.kind_of?(Hash)
    warn "Credentials file '#{@creds_file}' is not properly formatted! Aborting."
    exit 1
  end

  AWS.config(credentials)
end


op = OptionParser.new do |opts|
  opts.banner = <<-END.gsub(/^ {4}/, '')
    usage: 6k [OPTIONS] <command> [<type>] [<args>]

    commands:
       help           Display help information for a command
       launch         Launch new instance(s) of of type <type>
       start          Re-start stopped instance(s)
       stop           Stop running instance(s)
       terminate      Terminate running instance(s)
       list           List instances of type <type>

    Examples of common tasks:
        6k help launch
        6k launch worker --count 10 --processes 44 > w_ids.txt
        6k terminate --idfile w_ids.txt
        6k list all

    options:
    END
  opts.on("-c","--config CONFIG",
                    "Configuration details.",
                    "  Default: #{config}") do |c|
    config = c
  end
  opts.on("-r","--credentials CREDS_FILE",
               "Use credentials file CREDS_FILE.",
               "  Defaults to credentials.yml.") do |file|
    @creds_file = file
  end
end

begin op.order! argv
rescue OptionParser::InvalidOption => e
  # Normally, we would do what's below, but since we have stacked option parsers
  # this won't work. We just have to ignore invalid options and let the next
  # parser in the chain balk at them.
  puts e
  puts op
  exit 1
end

command = argv.size > 0 ? argv.shift : 'help'
case command
when 'help'
  help_target = argv.shift
  if !help_target
    puts op
    exit 0
  else
    begin
      SixK.method(help_target).call(config)
    rescue
      warn "No command named '#{help_target}'."
      puts ""
      puts op
      exit 1
    end
  end
when 'launch'
  setup_AWS
  SixK.launch(config,argv)
when 'start'
  setup_AWS
  SixK.start(config,argv)
when 'stop'
  setup_AWS
  SixK.stop(config,argv)
when 'terminate'
  setup_AWS
  SixK.terminate(config,argv)
when 'list'
  setup_AWS
  SixK.list(config,argv)
else
  warn "No command named '#{command}'."
  puts ""
  puts op
  exit 1
end

exit 0

