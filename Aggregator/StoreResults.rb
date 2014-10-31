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
require 'logger'

class Rusle2Aggregator < SingletonApp


  # Keeps a single connection to the db open while we process everything. Only
  # called on the singleton-proper.
  def start_application
   
    lf = File.new('StoreResults.log', 'a')
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
        host: 'development-ezq-persistence.agsolver.com',
        dbname: 'praxik',
        user: 'app',
        password: 'app')
    end

    data_spec = DataSpecParser::get_data_spec('main.cxx')
    @tablename = data_spec[:tablename]
    @fields = data_spec[:fields]
    
    sql = "create table if not exists #{@tablename}( #{@fields} )"
    @db.exec( sql )
  end




  # Takes the json passed on the socket and pushes it into the db
  def handle_data_from_client(connection)
    data = {}
    return false if !get_json(connection,data)
    @log.info "Received data"
    if data.size == 1 and data.fetch('set_table',false)
      @tablename = data['set_table']
      @log.info "Changing main storage table to #{@tablename}"
      sql = "create table if not exists #{@tablename}( #{@fields} )"
      @db.exec( sql )
      return true
    end
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
    @log.error "Error parsing json."
    @log.error e
    return false
  end



  # Stores data in the db. Returns true if successful, and false otherwise
  # @param [hash] data Hash of data to store in the db
  def store_data(data)
    
    return batch_store_data(data) if data.fetch('batch_mode',false)
    
    non_spec_data = remove_elements_not_in_spec!(data)
    # Save inputs to db only if that flag was set in the data
    to_db(non_spec_data,data['job_id'],data.fetch('task_id','-1')) if non_spec_data.fetch('aggregator_store_inputs',false)
    bindings, cols, val_holders = [],[],[]
    data.each do |k,v|
      cols.push(k)
      bindings.push(v)
    end
    val_holders = (1..cols.size).to_a.map{|i| "$#{i}"}
    result = @db.exec_params( %[ INSERT INTO #{@tablename} (#{cols.join(',')})
                                 VALUES(#{val_holders.join(',')}) ],
                                 bindings)
    result.check
    return true
  rescue => e
    @log.error "Database error: #{e}"
    return false
  end


  # Stores a batch of data to the db in a single transaction. Returns true if
  # the transaction succeeds, and false otherwise. Unlike the single mode
  # store_data, we don't write any non-spec data to disk. Batch mode is
  # set up for speed, so we avoid any overhead that we can.
  def batch_store_data(data)
    @log.info "Batch store"
    record_keys = data.keys # - ['batch_mode','job_id']
    record_keys.delete_if{|k| !(k =~ /record_/) }
    @log.debug "#{record_keys.join(', ')}" 

    transaction_result = true
    @log.info "Begin transaction"
    @db.transaction do |conn|
      record_keys.each do |rk|
        record = data[rk]
        remove_elements_not_in_spec!(record)
        bindings, cols, val_holders = [],[],[]
        record.each do |k,v|
          cols.push(k)
          bindings.push(v)
        end
        @log.debug "Saving record"
        val_holders = (1..cols.size).to_a.map{|i| "$#{i}"}
        result = conn.exec_params( %[ INSERT INTO #{@tablename} (#{cols.join(',')})
                                   VALUES(#{val_holders.join(',')}) ],
                                   bindings)
        begin
          @log.debug "Checking result"
          result.check
        rescue => e
          @log.error "DB error inside transaction: #{e}"
          transaction_result = false
        end
      end
    end #transaction

    @log.info "Ended transaction with result #{transaction_result}"
    return transaction_result
  end



  # Removes kv pairs from data for which the key does not exist in the
  # data_spec. Returns an array of kv pairs.
  def remove_elements_not_in_spec!(data)
    non_spec_data = data.select{|k,v| !@fields.include?(k)}
    data.delete_if{|k,v| !@fields.include?(k)}
    return non_spec_data
  end



  # Writes the given data to a file intended to store all the non-spec data
  # This method breaks genericness of this class because the task_id key
  # must be present in the data stream and it makes all kinds of assumptions
  # about what needs to be in the written data. Rethink both this method and
  # how it is called.
  def to_disk(data,job_id)
    Dir.mkdir('report_data') unless Dir.exists?('report_data')
    File.open("report_data/#{job_id}_input_data.json", 'a') {|f| f.write("#{data['inputs'].to_json}\n####\n")}
  end


  def to_db(data,job_id,task_id)
    tablename = "_#{job_id.gsub('-','')}_inputs"
    fields = 'task_id int, inputs text'
    sql = "create table if not exists #{tablename}( #{fields} )"
    @db.exec( sql )
    result = @db.exec_params(%[ INSERT INTO #{tablename} (task_id, inputs) VALUES($1, $2) ],
            [task_id,data['inputs'].to_json])
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


#Rusle2Aggregator.new('127.0.0.1',:port=>5024,:listen=>true)
Rusle2Aggregator.new('/tmp/rusle2',:listen=>true)
