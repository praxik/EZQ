
# Unit Tests
if __FILE__ == $0

print 'Setting up AWS...'
AWS.config(YAML.load(File.read('credentials.yml')))
puts 'done'

# Send a message to a queue, ask to receive it back, check it.
def test_one
  digest = EZQ.enqueue_message( "Test data", {'EZQ'=>nil},'test_queue',true )
  print "Test one (enqueue a single message): "
  q = EZQ.get_queue('test_queue')
  if !q
    puts "failed to get test_queue"
    return nil
  end
  q.receive_message do |msg|
    if msg.md5 == digest
      puts "pass"
    else
      puts "fail"
    end
  end
  return nil
end

# Send a message that we know is way too big to a queue. Ensure it was diverted
# to S3 properly. Ask to receive message back from queue, and check the
# preamble.
def test_two
  digest = EZQ.enqueue_message((1..40000).to_a.to_yaml,{'EZQ'=>nil},'test_queue')
  print "Test two (enqueue an oversized message): "
  q = EZQ.get_queue('test_queue')
  if !q
    puts "failed to get test_queue"
    return nil
  end
  q.receive_message do |msg|
    pre = YAML.load(msg.body)
    if pre.has_key?('get_s3_file_as_body')
      puts 'pass'
    else
      puts 'fail'
    end
  end
  return nil
end


# Send a whole batch of messages at once.
def test_three
  print "Test three (enqueue batch of messages): "
  msg_ary = (1..14).map{|i| i.to_s}
  EZQ.enqueue_batch(msg_ary,[{'EZQ'=>nil}],'test_queue')
  q = EZQ.get_queue('test_queue')
  14.times do
    q.receive_message do |msg|
      msg_ary -= [EZQ.strip_preamble(msg.body)]
    end
  end
  puts msg_ary
  if msg_ary.empty?
    puts 'pass'
  else
    puts 'fail'
  end
end


def test_four
  print "Test four (send a file to s3 using bcf notation): "
  File.write('test.txt',"This is a test")
  thread = EZQ.send_bcf_to_s3_async("6k_test.praxik,test.txt")
  thread.join
  puts 'pass'
end

#test_one()
#test_two()
#test_three()
#test_four()


end
