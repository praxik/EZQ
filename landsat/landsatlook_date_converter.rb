
require 'date'

module LsDate

  # Returns a DateTime object when given a LandsatLook extended
  # timestamp, which is a Unix epoch value with three extra characters
  # tacked onto the end.
  # @param [Integer,String] ts LandsatLook extended timestamp
  # @return [DateTime] A standard DateTime object
  def LsDate.to_datetime(ts)
    # Slice characters from string rather than divide by 1000 in case
    # last 3 digits are not 0....
    return Time.at(ts.to_s.slice(0,ts.to_s.size - 3).to_i).to_datetime
  end

  # Instance method version of class method of same name;
  # nicer for mix-ins
  def to_datetime(ts)
    return LsDate.to_datetime(ts)
  end

end # module


#   # test
#   # use the module's static methods
#   puts LsDate.to_datetime(1412204076000).to_s

#   # Alternatively, mix-in to avoid qualifying the namespace
#   include LsDate
#   puts to_datetime(1412204076000).to_s
#   pass = to_datetime(1412204076000) == DateTime.parse('2014-10-01T17:54:36-05:00')
#   puts "Pass: #{pass}"
