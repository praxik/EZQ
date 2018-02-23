require 'yaml'

module EZQ

class Preamble

  attr_accessor :get_s3_files, :put_s3_files
  attr_accessor :result_queue_name
  attr_accessor :process_command

  def initialize(data={})
    pre = data
    if data.kind_of?(String)
      pre = YAML.load(data)
    end

    ezq = pre['EZQ']
    return nil if ezq.nil?

    @get_s3_files = ezq['get_s3_files']
    @put_s3_files = ezq['put_s3_files']
    @result_queue_name = ezq['result_queue_name']
    @process_command = ezq['process_command']
  end

  def to_h
    h = {"EZQ" => {}}
    ezq = h['EZQ']
    ezq['get_s3_files'] = @get_s3_files unless @get_s3_files.nil? || @get_s3_files.empty?
    ezq['put_s3_files'] = @put_s3_files unless @put_s3_files.nil? || @put_s3_files.empty?
    ezq['result_queue_name'] = @result_queue_name unless @result_queue_name.nil? || @result_queue_name.empty?
    ezq['process_command'] = @process_command unless @process_command.nil? || @process_command.empty?
    h
  end

  def to_yaml
    to_h.to_yaml
  end

end


class Message

  attr_accessor :preamble, :body

  def initialize(str='', file: '')
    if !file.empty?
      str = File.read(file)
    end

    if str.empty?
      @preamble = EZQ::Preamble.new
      @body = ''
    else
      @preamble = EZQ::Preamble.new(str)
      @body = str.sub(/-{3}\nEZQ.+?\.{3}\n/m,'')
    end
  end

  def to_yamldoc
    @preamble.to_yaml + "\n...\n" + @body
  end

end # class

end # module
