#! /usr/bin/env ruby

require 'json'
require 'aws-sdk'
require_relative 'extractors'

in_file = ARGV.shift

if !in_file
  puts "Error: No json file specified."
  puts ""
  puts "Usage:  file_dump JSON_FILE"
  puts ""
  exit(1)
end

input = JSON.parse(File.read(in_file))

AWS.config(YAML.load_file('credentials.yml'))

input.each{|field| Extractors.get_files(field)}
