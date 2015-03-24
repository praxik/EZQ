
mode = ARGV.shift

def output
  puts 'hello'
  puts 'world'
end

case mode
when nil
  exit 0
when 'success'
  output  
  exit 0
when 'failure'
  output
  exit 1
when 'fault'
  output
  raise "An exception!"
end
