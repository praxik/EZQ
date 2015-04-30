require 'minitest/autorun'
require 'logger'
require 'fileutils'
require 'json'
require_relative '../sqs.rb'


class TestEZQSQS < Minitest::Test

  def setup
    @queue_name = 'test_queue'
    @queue_url = Aws::SQS::Client.new().get_queue_url(queue_name: @queue_name).queue_url
    # @log = Logger.new(STDOUT)
    # @log.level = Logger::DEBUG
    # EZQ.set_logger(@log)
  end

  def get_message
    resp = Aws::SQS::Client.new().receive_message(queue_url: @queue_url).messages[0]
    Aws::SQS::Client.new().delete_message(queue_url: @queue_url, receipt_handle: resp.receipt_handle)
    return resp
  end

  def test_enqueue_message
    body = 'This is a test message'
    pre = {'EZQ'=>nil}
    EZQ.enqueue_message(body,pre,@queue_name)
    assert_equal pre.to_yaml + "...\n" + body, get_message.body
  end

  def test_prepare_message_for_queue
    body = 'This is a test message'
    pre = {'EZQ'=>{'s3_files'=>[{'bucket'=>'test','key'=>'file'},{'bucket'=>'test','key'=>'file2'}]}}
    new_body = EZQ.prepare_message_for_queue(body,pre)
    assert_equal "#{pre.to_yaml}...\n#{body}", new_body
  end

  def test_fill_array
    ary = [1,2,3,4]
    assert_equal [1,2,3,4,'a','a','a'], EZQ.fill_array(ary,7,'a')
  end

end
