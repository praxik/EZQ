# This script shoves code into the head of a file to parse a config file and
# set up some common variables.

require 'yaml'
require 'aws-sdk'

class Configulator
  attr_accessor :queue_name
  attr_accessor :s3_bucket
  attr_accessor :uri
  def initialize
    filename = '../config_for_tests.yml'

    puts "Parsing configuration file #{filename}"

    config_file = File.join(File.dirname(__FILE__),filename)
    if !File.exist?(config_file)
      raise "File #{filename} does not exist."
    end

    config = YAML.load(File.read(config_file))

    unless config.kind_of?(Hash)
      raise "File #{filename} is formatted incorrectly."
    end

    credentials = {}
    credentials['access_key_id'] = config['access_key_id']
    credentials['secret_access_key'] = config['secret_access_key']
    AWS.config(credentials)

    @queue_name = config['queue_name']
    @s3_bucket = config['s3_bucket']
    @uri = config['uri']
  end
end

