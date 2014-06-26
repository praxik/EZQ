#!/usr/bin/env ruby

# DataSpecParser provides an easy way to extract a tablename and data schema
# from a (usually C++) file containing special markup. The markup must follow
# the following rules:
#
# 1. Only one data spec is allowed per file.
# 2. The specification block must begin with the comment
#    `// BEGIN DATA SPEC` on a single line
# 3. The specification block must end with the comment
#    `// END DATA SPEC`
# 4. Inside the specification block, there must be one (and only one) comment
#    providing the relevant tablename like this:
#    `// tablename: my_table`
# 5. Fields are noted by placing them into a map or DynamicStruct named
#    `results`, with an **inline** comment giving the data type of the field.
#    For Example:
#    results[ "field_one" ] = 74.32 // FLOAT
#    This line will result in the creation of a field named "field_one" having
#    type FLOAT. The type is not case-sensitive, but should correspond to a
#    valid database type -- eg. INT, FLOAT, TEXT, VARCHAR, BOOL, etc.
#
# This markup can also be used in other languages by using an appropriate
# commenting strategy to mimic C++ comments, and ensuring the use of
# double quotes around field names. For example, in Ruby:
#
#     # // BEGIN DATA SPEC
#     # // tablename: candy
#     results[ "user_id" ] = 1234 # // INT
#     results[ "peppermint" ] = "yummy" # // TEXT
#     results[ "marshmallow" ] = "gross" # // TEXT
#     # // END DATA SPEC
#
# You are strongly encouraged to use this class as a standalone executable
# (it's already set up that way to make it easy) to verify that you have
# specified the tablename and field specs correctly.

class DataSpecParser

  # Parses a cxx file, looking for a data spec defining db table information
  # Returns a hash of the form
  # 

  # Parses a cxx file, looking for a data spec defining db table information
  # @param [String] filename Name of cxx file to parse for data spec.
  # @return [Hash] {:tablename => tablename, :fields => field_string}
  #                where field_string is a string of the form
  #                job_id text, name_0 type_0, name_1 type_1, etc.
  def self.get_data_spec(filename)
    lines = File.readlines(filename)
    # Walk through lines, looking for the block between BEGIN DATA SPEC and
    # END DATA SPEC, and store that block into a new array
    spec_block = []
    done = false
    in_spec = false
    lines.each do |line|
      if !in_spec
        next unless line =~ /BEGIN DATA SPEC/i
        in_spec = true
      else
        if line =~ /END DATA SPEC/i
          done = true
        else
          spec_block << line.strip
        end
      end
      break if done
    end

    # Extract tablename and field names and types
    tablename = ''
    fields = []
    spec_block.each do |line|
      if line =~ /tablename:/i
        tablename = line.slice(line.index(/tablename:/i)+10,line.length).strip
      end
      if line =~ /results\[/
        name, type = '',''
        /results\[\s*"(?<name>.*?)"\s*\].*\/\/\s*(?<type>.*)/ =~ line
        fields << [name, type.downcase]
      end
    end

    raise "Tablename not specified in #{filename}" if tablename.empty?
    raise "Fields not specified in #{filename}" if fields.empty?
    
    # Form the string
    # "job_id text, name_0 type_0, name_1 type_1, etc."
    fs = "job_id text, "
    fields.each {|f| fs << "#{f[0]} #{f[1]}, " }
    fs.strip!.slice!(fs.length - 1) # chomp the terminal comma

    return {:tablename => tablename, :fields => fs}
  end

end

################################################################################
# Run this bit if this file is being run directly as an executable rather than 
# being imported as a module.
if __FILE__ == $0
  require 'optparse'
  op = OptionParser.new do |opts|
    opts.banner = "Usage: data_spec_parser.rb filename"
  end

  begin op.parse!
  rescue OptionParser::InvalidOption => e
    if !quiet
      puts e
      puts op
    end
    exit 1
  end

  filename = ARGV.shift

  if filename == nil
    puts 'You must specify a filename.'
    puts ''
    puts op
    exit 1
  end

  spec = DataSpecParser::get_data_spec(filename)
  puts "Tablename: #{spec[:tablename]}"
  puts "Fields:"
  puts spec[:fields].split(', ')
  
end
################################################################################
