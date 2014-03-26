#!/usr/bin/env ruby

# Script to assist in starting AWS instances for the 6k workflow

# Just run this as
# ./start_instance.rb --help
# to get a description of the options.

require 'bundler/setup'
require 'yaml'
require 'aws-sdk'
require 'optparse'

# Allow choice from these AMIs only
imgs = { 'worker'        => 'ami-43a9bd2a',
         'pregrid'       => '????????????',
         'reporthandler' => '????????????' }

# Defaults for values than can be overridden on commandline
type = ''                 # worker, pregrid, reporthandler
count = 1                 # number of identical instances to start up at once
processes = 1             # number of processes to start up on each machine
size = 't1.micro'         # machine size on which to run
creds_file = '../credentials.yml' # credentials file

# These cannot be overridden on commandline
vpc_id = 'vpc-e894b787'
vpc_subnet = 'subnet-fc94b793'
security_groups = ['dev-persistence-server','dev-kibitz-worker']


op = OptionParser.new do |opts|
  opts.banner = "Usage: start_instance.rb TYPE [OPTIONS]",
                "  where TYPE is one of [worker, pregrid, reporthandler]",
                ""
  opts.on("-c","--count [N]",
                "Number of instances to start up.",
                "  Default: 1") do |c|
    count = c
  end
  opts.on("-p","--processors [N]",
                "# processes to start on each instance.",
                "  Default: 1") do |p|
    processes = p
  end
  opts.on("-s","--size [SIZE]",
                "AWS instance size.",
                "  One of [t1.micro, m1.small, m1.medium,",
                "  m1.large, m1.xlarge, c3.8xlarge].",
                "  Default: t1.micro") do |s|
    size = s
  end
  opts.on("-r","--credentials [CREDS_FILE]",
               "Use credentials file CREDS_FILE.",
               "  Defaults to credentials.yml.") do |file|
    creds_file = file
  end
  opts.on_tail("-h", "--help", "Show this message") do
    puts opts
    exit
  end
end

begin op.parse! ARGV
rescue OptionParser::InvalidOption => e
  puts e
  puts op
  exit 1
end

type = ARGV[0]

if type == nil || type.empty?
  warn "You MUST specify a TYPE"
  puts op
  exit 1
end

if !File.exists?(creds_file)
  warn "Credentials file '#{creds_file}' does not exist! Aborting."
  exit 1
end

credentials = YAML.load(File.read(creds_file))
if !credentials.kind_of?(Hash)
  warn "Credentials file '#{creds_file}' is not properly formatted! Aborting."
  exit 1
end

option_hash = { :image_id => imgs[type],
                :subnet => vpc_subnet,
                :security_groups => security_groups,
                :instance_type => size,
                :count => count,
                :user_data => "processes: #{processes}" }

AWS.config(credentials)
instances = Array(AWS::EC2.new.instances.create(option_hash))

# Slap a Name tag on each of these instances
ec2 = AWS::EC2.new
instances.each do |inst|
  ec2.tags.create(inst, 'Name', :value => "6k_#{type}")
end

# We could block here until each instance returns status == :running.
# Not sure whether that's desirable.
