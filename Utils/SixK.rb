
require 'bundler/setup'
require 'yaml'
require 'aws-sdk'
require 'optparse'

class SixK

  def initialize
    
  end


################################################################################

  # Launch a new worker, pregrid, or reporthandler instance
  def self.launch(argv=[])
    args = Array(argv)

    # Allow choice from these AMIs only
    imgs = { 'worker'        => 'ami-d1170bb8',
             'pregrid'       => 'ami-d1170bb8',
             'reporthandler' => 'ami-1352417a' }

    # These cannot be overridden on commandline
    vpc_id = 'vpc-e894b787'
    vpc_subnet = 'subnet-fc94b793'
    security_groups = ['dev-base','dev-kibitz-worker']

    # Options that can be orverridden on commandline
    count = 1                 # number of instances to start up simultaneously
    processes = 1             # number of processes to start up on each machine
    size = 't1.micro'         # machine size on which to run
    name = ''                 # name with which to tag instance(s)
    manage = false            # whether to start up a management instance
                              # of a functioning _type_ instance
    
    op = OptionParser.new do |opts|
      opts.banner = <<-END.gsub(/^ {8}/, '')
        Usage: 6k [options] launch <type> [<args>]
          where type is one of [worker, pregrid, reporthandler]

        The launch command outputs the AWS id of each launched instance onto a
        separate line on STDOUT. Capturing these ids in a file using shell
        redirection will allow you to keep track of the launched instances for
        later termination.

        Args:
        END
      opts.on("-c","--count N",
                    "Number of instances to start up.",
                    "  Default: 1") do |c|
        count = c
      end
      opts.on("-n","--name NAME",
                    "Name with which to tag the instance(s)",
                    "  Default: \"6k_TYPE\"" ) do |n|
        name = name
      end
      opts.on("-p","--processors N",
                    "# processes to start on each instance.",
                    "  Default: 1") do |p|
        processes = p
      end
      opts.on("-s","--size SIZE",
                    "AWS instance size.",
                    "  One of [t1.micro, m1.small, m1.medium,",
                    "  m1.large, m1.xlarge, c3.8xlarge].",
                    "  Default: t1.micro") do |s|
        size = s
      end
      opts.on("-m","--manage",
                    "Starts an instance lacking the userdata",
                    "  needed by bootstrap. This allows you",
                    "  to alter the base AMI and save as a new",
                    "  AMI.") do |p|
        manage = true
      end
    end

    # User issued 6k help launch
    if args.empty?
      puts op
      exit 0
    end

    begin op.parse! args
    rescue OptionParser::InvalidOption => e
      puts e
      puts op
      exit 1
    end

    if args.empty?
      warn "Error: An instance TYPE must be specified."
      puts ""
      puts op
      exit 0
    end

    type = args.shift
    allowable = ['worker','pregrid','reporthandler']
    if !allowable.include?(type)
      warn "Invalid type \"#{type}\"; must be one of #{allowable}"
      exit 1
    end

    userdata = {}
    userdata['deploy_bucket'] = '6k_test.praxik'
    case type
    when 'worker'
      userdata['deploy_key'] = 'arks/default/worker.zip'
    when 'pregrid'
      userdata['deploy_key'] = 'arks/default/pregrid.zip'
    when 'reporthandler'
      userdata['deploy_key'] = 'arks/default/reporthandler.zip'
    end
    userdata['number_of_processes'] = processes

    # Clearing out the userdata ensures that bootstrap exits due to lack
    # of info about what to start up.
    userdata = {} if manage


    option_hash = { :image_id => imgs[type],
                :subnet => vpc_subnet,
                :security_groups => security_groups,
                :instance_type => size,
                :count => count,
                :user_data => userdata.to_yaml }

    instances = Array(AWS::EC2.new.instances.create(option_hash))

    # Slap a Name and Type tag on each of these instances
    name = "6k_#{type}" if name.empty?
    name = "#{name}_manage" if manage
    ec2 = AWS::EC2.new
    instances.each do |inst|
      ec2.tags.create(inst, 'Name', :value => name)
      ec2.tags.create(inst, 'Type', :value => "6k_#{type}")
    end

    # Print the id of each instance to stdout, allowing redirection into a file
    # for later termination.
    instances.each {|inst| puts inst.id}
  end



################################################################################
  def self.start(argv=[])
    self.stopinate('start',argv)
  end
################################################################################
  def self.stop(argv=[])
    self.stopinate('stop',argv)
  end
################################################################################
  def self.terminate(argv=[])
    self.stopinate('terminate',argv)
  end
################################################################################

  # Start | Stop | Terminate instance(s)
  def self.stopinate(action,argv=[])
    args = Array(argv)

    if !['start','stop','terminate'].include?(action)
      warn "Error: No such action as #{action}."
      warn "How did this even get called?"
      exit 1
    end

    # Options that can be orverridden on commandline
    id_list = []
    id_file = ''
    ip_list = []
    ip_file = ''
    type = ''
    no_prompt = false

    op = OptionParser.new do |opts|
      opts.banner = <<-END.gsub(/^ {8}/, '')
        Usage: 6k [options] #{action}  [--ids ID1,ID2,...] [--idfile FILE] 
                                     [--ips IP1,IP2,...] [--ipfile FILE]
                                     [--type TYPE] [--yes]

        Args:
        END
      opts.on("-i","--ids LIST", Array,
                    "List of instance IDs to #{action}.") do |i|
        id_list = i
      end
      opts.on("-f","--idfile FILE",
                    "File containing list of instance IDs to",
                    "#{action}.") do |f|
        id_file = f
      end
      opts.on("-p","--ips LIST", Array,
                    "List of instance IP addresses to ",
                    "#{action}.") do |i|
        ip_list = i
      end
      opts.on("-g","--ipfile FILE",
                    "File containing list of instance IP",
                    "addresses to #{action}.") do |f|
        ipfile = f
      end
      opts.on("-t","--type TYPE",
                    "#{action} all instances of type TYPE.",
                    "  TYPE must be one of",
                    "  [all,worker,pregrid,reporthandler]",
                    "  'all' matches any of the other three",
                    "  types. If this option is specified in",
                    "  conjunction with any of the id* or ip*",
                    "  options, it will be used to further",
                    "  filter the list of instances to ",
                    "  #{action}. If this option is specified",
                    "  without the ip* or id* options, all",
                    "  instances of this type will be",
                    "  #{action}-ed.") do |t|
        type = t
      end
      opts.on("-y","--yes",
                    "Do not prompt for confirmation before ",
                    "#{action}-ing instances.") do |y|
        no_prompt = y
      end
    end

    # User issued 6k help start|stop|terminate
    if args.empty?
      puts op
      exit 0
    end

    begin op.parse! args
    rescue OptionParser::InvalidOption => e
      puts e
      puts op
      exit 1
    end

    if id_list.empty? and id_file.empty? and ip_list.empty? and ip_file.empty? and type.empty?
      warn "Error: Must specify one or more of [--ids, --idfile, --ips, --idfile]"
      puts ''
      puts op
      exit 1
    end

    instances = AWS::EC2::InstanceCollection.new
    if !type.empty?
      allowable = ['all','worker','pregrid','reporthandler']
      if !allowable.include?(type)
        warn "Invalid type \"#{type}\"; must be one of #{allowable}"
        exit 1
      end
      type = Array(type)
      type = ['worker','pregrid','reporthandler'] if type == ['all']
      type.map! {|e| "6k_#{e}"}
      # Get instances limited to those with a Type tag matching one of our
      # allowed tags. This prevents us from being able to accidentally
      # stop or terminate a non-6k instance. This protection only works if a 
      # type was sepcified, though!
      filter_string = ''
      type.each do |t|
        filter_string += "'#{t}',"
      end
      filter_string.slice!(filter_string.length - 1) # chomp the terminal comma
      instances = eval("instances.filter('tag:Type',#{filter_string})")
    end

    # Gather together all ids from both possible id sources
    begin
      if !id_file.empty?
        id_list << File.readlines(id_file).map {|l| l.strip}
      end
    rescue
      warn "Error opening or reading #{id_file}. Aborting."
      exit 1
    end

    # Gather together all ips from both possible ip sources
    begin
      if !ip_file.empty?
        ip_list << File.readlines(ip_file)
      end
    rescue
      warn "Error opening or reading #{ip_file}. Aborting."
      exit 1
    end

    # Filter instances by id and ip
    instances = instances.filter('private-ip-address',ip_list) if !ip_list.empty?
    instances = instances.filter('instance-id',id_list) if !id_list.empty?


    # Select only the running or pending ones if halting
    if ['stop','terminate'].include?(action)
      instances = instances.filter('instance-state-name','running','pending')
    else # Or stopped ones if starting
      instances = instances.filter('instance-state-name','stopped')
    end

    
    # Really start|stop|terminate these?
    unless no_prompt
      verbed = ''
      case action
      when 'start'
        verbed = 'started'
      when 'stop'
        verbed = 'stopped'
      when 'terminate'
        verbed = 'terminated'
      end
      puts "The following instances will be #{verbed}:"
      instances.each {|i| puts "%-10s  %-30s  %-12s  %-10s" % [i.id,
                          i.tags['Name'],i.private_ip_address,i.instance_type] }
      puts ""
      print 'Continue? ([y]/n): '
      resp = STDIN.gets.chomp[0]
      resp.downcase! if resp && !resp.empty?
      if resp == 'n'
        puts "Taking no action and exiting."
        exit 0
      end
    end

    case action
    when 'start'
      instances.each {|i| i.start}
      puts 'Started instances.'
    when 'stop'
      instances.each {|i| i.stop}
      puts 'Stopped instances.'
    when 'terminate'
      instances.each {|i| i.terminate}
      puts 'Terminated instances.'
    else
      warn 'Error: This branch of stopinate should never be called.'
    end
    
  end



################################################################################



  def self.list(argv=[])
    args = Array(argv)

    status_flags = []
    
    op = OptionParser.new do |opts|
      opts.banner = <<-END.gsub(/^ {8}/, '')
        Usage: 6k list TYPE [options]
          where TYPE is one of [all, worker, pregrid, reporthandler]

        The options p, r, s, and t can be combined into a single switch, eg.
          6k list all -pr
          6k list all -tsrp

        Omitting the p, r, s, and t options entirely has the same effect as
          specifying all of them.

        Options:
        END
      opts.on("-p","--pending",
                    "List pending instances.") do |f|
        status_flags << :pending
      end
      opts.on("-r","--running",
                    "List running instances.") do |f|
        status_flags << :running
      end
      opts.on("-s","--stopped",
                    "List stopped/stopping instances.") do |f|
        status_flags << :stopped << :stopping
      end
      opts.on("-t","--terminated",
                    "List terminated/shutting-down instances.") do |f|
        status_flags << :terminated << :shutting_down
      end
    end

    # User issued 6k help list
    if args.empty?
      puts op
      exit 0
    end

    begin op.parse! ARGV
    rescue OptionParser::InvalidOption => e
      puts e
      puts op
      exit 1
    end

    type = Array(args.shift)
    if type == nil || type.empty? || type[0] == nil
      warn "Error: You MUST specify a TYPE"
      puts op
      exit 1
    end
    allowable = ['all','worker','pregrid','reporthandler']
    if (allowable & type).empty?
      warn "Invalid type \"#{type}\"; must be one of #{allowable}"
      exit 1
    end
    type = ['worker','pregrid','reporthandler'] if type == ['all']
    # Prepend '6K_' onto the list of types.
    type.map! {|e| "6k_#{e}"}

    status_replacements = { :pending => 'P',
                            :running => 'R',
                            :shutting_down => 'ShD',
                            :terminated => 'T',
                            :stopping => 'Sg',
                            :stopped => 'S' }

    if status_flags.empty?
      status_flags =
               [:pending,:running,:shutting_down,:terminated,:stopping,:stopped]
    end
                            
    ec2 = AWS::EC2.new
    all_inst = ec2.instances.filter('tag-key','Type').reduce({}) do |h, i|
      if type.include?( i.tags['Type'] ) and status_flags.include?(i.status)
        h[i.id] = [i.tags['Name'],
                   status_replacements[i.status],
                   i.private_ip_address,
                   i.instance_type]
      end
      h        
    end

    if !all_inst || all_inst.empty?
      puts 'No matching instances.'
      exit 0
    end

    puts '---------------------------------------------------------------------'
    puts "%-10s  %-30s  %-3s  %-12s  %-10s" %
      ['ID','Name','State','Priv. IP','Size']
    puts '---------------------------------------------------------------------'
    all_inst.each do |k,v|
      v.insert(0,k)
      str = "%-10s  %-30s  %-3s  %-12s  %-10s" % v
      puts str
    end
      
  end
  
################################################################################

end
