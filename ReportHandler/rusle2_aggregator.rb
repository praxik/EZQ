#!/usr/bin/env ruby

# The message passed to this program on the commandline will be parsed and
# placed into a PostgreSQL database.
require 'bundler/setup'
require 'pg'
require 'json'
require 'socket'
require 'tmpdir'
require './singleton_app'
require './data_spec_parser.rb'

class Rusle2Aggregator < SingletonApp


  # Keeps a single connection to the db open while we process everything. Only
  # called on the singleton-proper.
  def start_application

   
    lf = File.new('rusle2_aggregator.log', 'a')
    lf.sync = true
    @log = Logger.new(lf)
    $stderr = lf
    @log.level = Logger::INFO
    # Any commandline arg to the first instance will be interpreted as a local
    # database name so we can do local testing.
    if ARGV[0]
      @db = PG.connect(dbname: ARGV[0])
    else
      @db = PG.connect(
        host: 'development-rds-pgsq.csr7bxits1yb.us-east-1.rds.amazonaws.com',
        dbname: 'praxik',
        user: 'app',
        password: 'app')
    end

    data_spec = DataSpecParser::get_data_spec('main.cxx')
    @tablename = data_spec[:tablename]
    
    sql = "create table if not exists #{@tablename}( #{data_spec[:fields]} )"
    @db.exec( sql )
  end




  # Takes the json passed on the socket and pushes it into the db
  def handle_data_from_client(connection)
    data = {}
    return false if !get_json(connection,data)
    @log "Rec'd: #{data}"
    #puts "Rec'd: #{data}"
    return store_data(data)
  end




  # Reads the data from the socket and parses as json. Returns true if all OK.
  # @param [Socket] connection The socket connection to use for getting data
  # @param [Hash] json_result Successfully parsed json is placed in this hash
  def get_json(connection,json_result)
    data = connection.read
    json_result.replace(JSON.parse(data)) # Have to do #replace rather than
                                          # = due to Ruby's pass-ref-by-value
                                          # semantics.
    return true
  rescue => e
    warn e
    warn "Error parsing json."
    return false
  end




  # Stores data in the db. Returns true if successful, and false otherwise
  # @param [hash] data Hash of data to store in the db
  def store_data(data)
    bindings, cols, val_holders = [],[],[]
    data.each_with_index do |(k,v),i|
      cols.push(k)
      val_holders.push("$#{i+1}")
      bindings.push(v)
    end
    result = @db.exec_params( %[ INSERT INTO #{@tablename} (#{cols.join(',')})
                                 VALUES(#{val_holders.join(',')}) ],
                                 bindings)
    result.check
    return true
  rescue => e
    warn e
    warn "Database error"
    return false
  end




  # Sends the first cmdline arg to the singleton unless -f specified, in
  # which case it opens the specified file and sends the contents to the
  # singleton.
  def send_data_to_singleton(connection)
    return if ARGV[0] == nil
    if ARGV[0] == '-f'
      file = ARGV[1]
      return if file == nil
      msg = File.read(file)
      connection.puts(msg)
    else
      connection.puts(ARGV[0])
    end
  end
  
end


Rusle2Aggregator.new('127.0.0.1',:port=>5024,:listen=>true)
