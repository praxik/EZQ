#!/usr/bin/env ruby

require '../ezqlib'

path = ARGV.shift
Dir.glob(path).each do |file|
  puts "Getting files associated with #{file}"
  preamble = EZQ.extract_preamble(File.read(file))
  preamble['get_s3_files'].each do |ref|
    EZQ.get_s3_file(ref['bucket'],ref['key'])
  end
end
