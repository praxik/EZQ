
module NumberFormatters

  # Convert an Integer or Float to String in x,xxx.yy format (like currency)
  # @param [Integer,Float] v Value to convert
  #
  @return [String]
  def self.curr(v)
    return commafy("%.2f" % v.to_f)
  end


  # Convert an Integer or Float to integer String in x,xxx format
  # @param [Integer,Float] v Value to convert
  # @return [String]
  def self.int(v)
    return commafy("%i" % v.to_f.round)
  end


  # Convert an Integer or Float to String in x,xxx.00 format
  # (fractional part is locked to 00)
  # @param [Integer,Float] v Value to convert
  # @return [String]
  def self.curr_int(v)
    return curr(v.to_f.round)
  end



  # Returns a String version of an Integer or Float. The string contains
  # commas in the places we use those in SAE or SBE.
  # @param [Integer,Float] v Value to convert
  # @return [String]
  def self.commafy(v)
    whole,fraction = v.to_s.split('.')
    fraction = '' if fraction == nil
    f_sep = fraction == '' ? '' : '.'
    sign = whole[0] == '-' ? whole.slice!(0) : ''
    # Add commas for each group of 3 digits, counting right to left
    whole = whole.reverse.split(/([\d]{3})/).delete_if{|c| c==''}.join(',').reverse
    return sign + whole + f_sep + fraction
  end

end # module