
require 'bundler/setup'
require 'yaml'
require 'aws-sdk'
require 'optparse'
require 'base64'
require 'json'
require './instance_info_collection'

class SixK

  def initialize
    
  end

################################################################################

  # Parse the config and set up the parts that never get overridden
  def self.load_config(filename)
    if !File.exists?(filename)
      warn "Error: Unable to open configuration file '#{filename}'."
      exit 1
    end
    @config = YAML.load(File.read(filename))

    @imgs = {}
    @config['ami_map'].each { |k,v| @imgs[k] = v }

    @types = @imgs.keys
    @spot_subnets = @config['spot_subnets']
  end

################################################################################

  # Finish setting up the configuration based on the type being used.
  def self.setup_by_type(type)
    return nil if !@types.include?(type)

    security = @config['security']['base']
    if @config['security'].has_key?(type)
      security.merge!(@config['security'][type])
    end
    @vpc_id = security['vpc_id']
    @vpc_subnet = security['vpc_subnet']
    @security_groups = security['security_groups']

    launch_settings = @config['launch_settings']['base']
    if @config['launch_settings'].has_key?(type)
      launch_settings.merge!(@config['launch_settings'][type])
    end
    @size = launch_settings['size']
    @count = launch_settings['count'].to_i

    @userdata = @config['userdata']['base']
    if @config['userdata'].has_key?(type)
      @userdata.merge!(@config['userdata'][type])
    end
  end

################################################################################

  # Launch a new worker, pregrid, or reporthandler instance
  def self.launch_or_spot(action='launch',config='',argv=[])
    args = Array(argv)

    self.load_config(config) if !config.empty?

    # Options that can be orverridden on commandline
    cl_count = -1
    cl_processes = -1
    cl_size = ''
    cl_name = ''
    cl_manage = false
    
    op = OptionParser.new do |opts|
      opts.banner = <<-END.gsub(/^ {8}/, '')
        Usage: 6k [options] #{action} <type> [<args>]
          where type is one of #{@types}.

        The launch command outputs the AWS id of each launched instance onto a
        separate line on STDOUT. Capturing these ids in a file using shell
        redirection will allow you to keep track of the launched instances for
        later termination.

        Args:
        END
      opts.on("-c","--count N",
                    "Number of instances to start up.",
                    "  Default: 1") do |c|
        cl_count = c.to_i
      end
      if action == 'launch'
        opts.on("-n","--name NAME",
                      "Name with which to tag the instance(s)",
                      "  Default: \"6k_TYPE\"" ) do |n|
          cl_name = name
        end
      end
      opts.on("-p","--processors N",
                    "# processes to start on each instance.",
                    "  Overrides the setting in config file." ) do |p|
        cl_processes = p.to_i
      end
      opts.on("-s","--size SIZE",
                    "AWS instance size.",
                    "  One of [t1.micro, m1.small, m1.medium,",
                    "  m1.large, m1.xlarge, c3.8xlarge].",
                    "  Overrides setting in config file.") do |s|
        cl_size = s
      end
      if action == 'launch'
        opts.on("-m","--manage",
                      "Starts an instance without the userdata",
                      "  needed by bootstrap. This allows you",
                      "  to alter the base AMI and save as a new",
                      "  AMI.") do |p|
          cl_manage = true
        end
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
      exit 1
    end

    type = args.shift
    if !@types.include?(type)
      warn "Error: Invalid type \"#{type}\"; must be one of #{@types}"
      exit 1
    end

    self.setup_by_type(type)

    # Overrride settings based on commandline args
    @count = cl_count if cl_count > 0
    @count = 1 if @count <= 0
    @size = cl_size if !cl_size.empty?
    @size = 't1.micro' if @size.empty?
    name = cl_name
    np = @userdata.fetch('number_of_processes',1)
    np = cl_processes if cl_processes > 0
    np = 1 if np != 'auto' and np.to_i <= 0

    # Clearing out the userdata ensures that bootstrap exits due to lack
    # of info about what to start up.
    @userdata = {} if cl_manage

    #puts @userdata.to_yaml
    #exit 0

    case action
    when 'launch'
      launch(name,type)
    when 'spot'
      spot(type)
    end

    
  end


################################################################################
  def self.launch(name,type)
    option_hash = { :image_id => @imgs[type],
                :subnet => @vpc_subnet,
                :security_groups => @security_groups,
                :instance_type => @size,
                :count => @count,
                :user_data => @userdata.to_yaml }

    instances = Array(AWS::EC2.new.instances.create(option_hash))

    # Slap a Name and Type tag on each of these instances
    name = "6k_#{type}" if name.empty?
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
  def self.spot(type)
    ec2 = AWS::EC2.new

    puts ''
    puts "Choose subnets across which to span bid: "
    @spot_subnets.each_with_index do |sn,idx|
      ips = ec2.subnets[sn].available_ip_address_count()
      name = ec2.subnets[sn].tags['Name']
      cidr = ec2.subnets[sn].cidr_block()
      info = [idx+1,sn,name,cidr,ips]
      puts "%-2d:  %-15s  %-15.15s  %-15.15s  %-3d free ips" % info
    end
    print "Enter as a comma-separated list of numbers: "
    subnet_indices = STDIN.gets.chomp.split(',').map{|i| i.strip.to_i}
    subnets = subnet_indices.map{|i| @spot_subnets[i-1]}

    n_ips = subnets.reduce(0){|acc,s| acc += ec2.subnets[s].available_ip_address_count()}
    
    #avail_ips = ec2.subnets[@vpc_subnet].available_ip_address_count()

    puts ''
    print "Number of instances to bid (0 - #{n_ips}): "
    count = STDIN.gets.chomp.to_i
    if count <= 0 or count > n_ips
      puts "Taking no action and exiting."
      exit 0
    end

    price_specs = {:instance_types=>[@size],
        :product_descriptions=>['Windows','Linux/UNIX'],
        :availability_zone=>'us-east-1a',
        :start_time=>Time.now.gmtime.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        :end_time=>Time.now.gmtime.strftime("%Y-%m-%dT%H:%M:%S.000Z")}

    prices = ec2.client.describe_spot_price_history(price_specs)

    puts ''
    puts "Current spot prices for #{@size} are:"
    prices[:spot_price_history_set].each{|it| puts "\t#{it[:product_description]}: $#{it[:spot_price]}"}
    print 'Bid price ($): '
    price = STDIN.gets.chomp

    puts ''
    print "#{count} instances at $ %2.5f will cost $ %5.2f per hour. Continue? [y/N]: " % [price.to_f,count*price.to_f]
    cont = STDIN.gets.chomp
    if cont.downcase != 'y'
      puts "Taking no action and exiting."
      exit 0
    end

    # When using the low-level client interface, we have to convert security
    # group names to ids iff running in a subnet.
    sgs = ec2.security_groups.filter('group-name',*@security_groups)
    sgids = []
    sgs.each{|sg| sgids << sg.id}

    # Rather than doing a fair spread across the subnets, we fill up the subnets
    # in the order they were chosen.
    sn_index = 0
    while (count > 0) and (sn_index < subnets.size)
      sn_id = subnets[sn_index]
      specs = { :image_id => @imgs[type],
                :subnet_id => sn_id,
                :security_group_ids => sgids,
                :instance_type => @size,
                :user_data => Base64.encode64(@userdata.to_yaml) }

      av = ec2.subnets[sn_id].available_ip_address_count
      num_instances = (count >= av) ? av : count
      count = (count >= av) ? (count - av) : 0

      options = {:spot_price=>price,
                 :instance_count=>num_instances,
                 :type=>'one-time',
                 :launch_specification=>specs
                 }

      ec2.client.request_spot_instances(options)
      sn_index += 1
    end
    
  end



################################################################################
  def self.start(config='',argv=[])
    self.load_config(config) if !config.empty?
    self.stopinate('start',argv)
  end
################################################################################
  def self.stop(config='',argv=[])
    self.load_config(config) if !config.empty?
    self.stopinate('stop',argv)
  end
################################################################################
  def self.terminate(config='',argv=[])
    self.load_config(config) if !config.empty?
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
        ip_file = f
      end
      opts.on("-t","--type TYPE",
                    "#{action} all instances of type TYPE.",
                    "  TYPE must be one of",
                    "  #{@types + ['all']}",
                    "  'all' matches any of the other types",
                    "  If this option is specified in",
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

    filters = []
    if !type.empty?
      allowable = ['all'] + @types
      if !allowable.include?(type)
        warn "Invalid type \"#{type}\"; must be one of #{allowable}"
        exit 1
      end
      type = Array(type)
      type = @types if type == ['all']
      type.map! {|e| "6k_#{e}"}
      # Get instances limited to those with a Type tag matching one of our
      # allowed tags. This prevents us from being able to accidentally
      # stop or terminate a non-6k instance. This protection only works if a 
      # type was sepcified, though!
      filters << {:name=>"tag:Type",:values=>type}
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
    filters << {:name=>'private-ip-address',:values=>ip_list} if !ip_list.empty?
    filters << {:name=>'instance-id',:values=>id_list} if !id_list.empty?


    # Select only the running or pending ones if stopping
    case action
    when 'stop'
      filters << {:name=>'instance-state-name',:values=>['running',
                                                         'pending']}
    when 'terminate'
      filters << {:name=>'instance-state-name',:values=>['running',
                                                         'pending',
                                                         'stopped']}
    when 'start'
      filters << {:name=>'instance-state-name',:values=>['stopped']}
    end

    instances = Nimbus::InstanceInfoCollection.new.filter(filters)
    
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
      instances.start_all()
      puts 'Started instances.'
    when 'stop'
      instances.stop_all()
      puts 'Stopped instances.'
    when 'terminate'
      instances.terminate_all()
      puts 'Terminated instances.'
    else
      warn 'Error: This branch of stopinate should never be called.'
    end
    
  end



################################################################################



  def self.list(config='',argv=[])

    self.load_config(config) if !config.empty?
    args = Array(argv)

    status_flags = []
    ssh_connect = false

    op = OptionParser.new do |opts|
      opts.banner = <<-END.gsub(/^ {8}/, '')
        Usage: 6k list TYPE [options]
          where TYPE is one of #{@types + ['all']}

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
      opts.on("-c","--ssh_connect",
                    "Prompt for ssh connection to an instance.") do |f|
        ssh_connect = true
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
    allowable = ['all'] + @types
    if (allowable & type).empty?
      warn "Invalid type \"#{type}\"; must be one of #{allowable}"
      exit 1
    end
    type = @types if type == ['all']
    # Prepend '6K_' onto the list of types.
    type.map! {|e| "6k_#{e}"}

    # The short status tags we'll use for display in a list
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

    # List of available filters is at
    # http://docs.aws.amazon.com/cli/latest/reference/ec2/describe-instances.html
    # 'instance-state-name' filter works on strings, not symbols
    status_filter = status_flags.map{|s| s.to_s}
    filters = [{:name=>'instance-state-name',:values => status_filter},
               {:name=>'tag:Type',:values=>type}]
    instances = Nimbus::InstanceInfoCollection.new.filter(filters)
    disp_info = {}
    instances.each do |i|
      disp_info[i.id] = [ i.tags['Name'],
                          status_replacements[i.status],
                          i.private_ip_address,
                          i.type,
                          i.spot? ? 'T' : 'F' ]
    end

    if !disp_info || disp_info.empty?
      puts 'No matching instances.'
      exit 0
    end

    puts '--------------------------------------------------------------------------------'
    space = ssh_connect ? '   # ' : '' #Add padding if ssh numbers will appear
    puts "#{space}%-30s  %-3s  %-12s  %-10s  %-4s" %
      ['Name','Sts','IP','Size','Spt?']
    puts '--------------------------------------------------------------------------------'
    disp_info.each_with_index do |(k,v),idx|
      #v.insert(0,k) # Insert the image ID at the head of the value array
      if ssh_connect
        v.insert(0,idx+1)
        str = "%4d %-30.30s  %-3s  %-12s  %-10s  %-4s" % v
      else
        str = "%-30.30s  %-3s  %-12s  %-10s  %-4s" % v
      end
      puts str
    end

    connect(instances) if ssh_connect
      
  end


################################################################################
  def self.connect(instances)
    puts ''
    puts 'Enter # of instance to which to connect and press Enter.'
    puts 'Leave blank to exit without connecting to an instance.'
    print '> '
    n = gets

    if n.strip.empty?
      puts 'Exiting without making a connection.'
      exit 0
    else
      conn_to = n.strip.to_i
      if conn_to < 1 or conn_to > instances.size
        warn "#{conn_to} is not a valid instance number."
        exit 1
      end
      ssh_config = YAML.load(File.read('nimbus_ssh.yml'))

      ec2 = AWS::EC2.new
      inst = instances[conn_to-1]
      i_type = inst.tags['Type'].gsub(/^6k_/,'')

      flags = ssh_config['base']
      # Default to tunnel if windows, shell otherwise
      flags['action'] = inst.platform() == 'windows' ? 'tunnel' : 'shell'
      # action can still be overridden by setting in type-specific config
      flags.merge!(ssh_config[i_type]) if ssh_config.has_key?(i_type)

      p_user = flags['proxy_user']
      p_id = flags['proxy_identity']
      p_ip = flags['proxy_ip']
      h_user = flags['host_user']
      h_id = flags['host_identity']
      h_ip = inst.private_ip_address()
      l_p = flags['local_tunnel_port']
      h_p = flags['host_tunnel_port']
      
      case flags['action']
      when 'tunnel'
        puts "Setting up tunnel to #{h_ip} mapping port #{h_p} to local port #{l_p}"
        ssh_cmd = "ssh -i #{p_id} -L #{l_p}:#{h_ip}:#{h_p} #{p_user}@#{p_ip}"
      when 'shell'
        puts "Setting up shell for #{h_ip}"
        ssh_cmd = "ssh -i #{h_id} -o 'ProxyCommand=ssh -i #{p_id} #{p_user}@#{p_ip} nc -q0 #{h_ip} 22' #{h_user}@#{h_ip}"
      end
      system(ssh_cmd)
    end
  end
  
################################################################################

end
