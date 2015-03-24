
module EZQ

class Preamble

  # Create a new Preamble object from a
  # preamble string or hash
  def initialize(preamble)
    if preamble.kind_of?(String)
      @preamble = YAML.load(preamble)
    elsif preamble.kind_of?(Hash)
      @preamble = preamble
    else
      raise "Can't create a Preamble from a {#preamble.name}"
    end
  end



  def process_directives(config)
  end



  def fetch_files
  end



  def get_expansion_hash
  end



  def get_endpoints
  end



  def has_body?
  end



  def get_body
  end



end

end