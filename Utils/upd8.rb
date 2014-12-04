#!/usr/bin/env ruby

require 'fileutils'

def make_changes
  FileUtils.cp('../processor_fan_out.rb','.')
end

ark = ARGV.shift

bbase = 's3://6k_test.praxik/arks/default'

puts "Getting #{bbase}/#{ark}"
system("aws s3 cp #{bbase}/#{ark} ark/.")

Dir.chdir('ark')

puts "Unzipping archive"
system("unzip #{ark}")

puts "Making changes to files"
make_changes()

FileUtils.rm(ark)

puts "Re-zipping archive"
system("zip -9 -r #{ark} *")

puts "Sending altered archive to S3"
system("aws s3 cp #{ark} #{bbase}/#{ark}")

puts "Removing temporaries"
system("rm -rf *")

