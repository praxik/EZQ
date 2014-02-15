#!/usr/bin/env ruby

# This script creates N subdirectories, numbered 0 through N, in the present
# working directory. It then copies the files specified in deployfiles.txt into
# each subdirectory.

require 'fileutils'

def print_usage
  puts <<-END
  Usage: deploy N
    where N is a natural number
    The file deployfiles.txt must exist in the present working directory.
    This file should contain a list of the files to be copied into each
    subdirectory, one file per line. Globbing is supported.
  END
  exit 1
end

print_usage if ARGV[0].to_i < 1
print_usage if !File.exists?('deployfiles.txt')
  
files = File.readlines('deployfiles.txt').map{|file| Dir[file.strip]}.flatten
n = ARGV[0].to_i

n.times do |idx|
  subdir = "%02d" % idx
  Dir.mkdir(subdir)
  FileUtils.cp_r(files,subdir)
end
