require 'minitest/autorun'
require 'logger'
require 'fileutils'
require 'json'
require_relative '../s3.rb'


class TestEZQS3 < Minitest::Test

  def setup
    @bucket = 'test.agsolver'
    # @log = Logger.new(STDOUT)
    # @log.level = Logger::DEBUG
    # EZQ.set_logger(@log)

    @wrong_contents = 'File contents are incorrect'
    @wrong_key = 'Returned key is incorrect'
    @no_file = 'File does not exist'
  end

  def clean_file(file)
    File.unlink(file) if File.exist?(file)
  end

  def test_get_s3
    refute_equal nil, EZQ.get_s3, 'Could not get S3 object'
  end


  def test_send_data_to_s3
    file = 'EZQ_unit_tests/test.txt'
    EZQ.remove_s3_file(@bucket,file)
    contents = 'Test data'

    key = EZQ.send_data_to_s3(contents, @bucket, file)
    EZQ.get_s3_file(@bucket, file)
    assert_equal file, key, @wrong_key
    assert File.exist?(file), @no_file
    assert_equal contents, File.read(file), @wrong_contents
  ensure
    FileUtils.rm_rf(file)
  end


  def test_send_data_to_s3_compressed
    file = 'EZQ_unit_tests/compress.txt'
    EZQ.remove_s3_file(@bucket,file)
    contents = 'Test data'

    key = EZQ.send_data_to_s3(contents, @bucket, file, compress: true)
    EZQ.get_s3_file(@bucket, "#{file}.gz", decompress: true)
    assert_equal file + '.gz', key, @wrong_key
    assert File.exist?(file), @no_file
    assert_equal contents, File.read(file), @wrong_contents
  ensure
    clean_file(file)
    clean_file("#{file}.gz")
  end


  def send_file(file,dir,key,compress)
    contents = 'Some test text'
    EZQ.remove_s3_file(@bucket,file)

    FileUtils.mkdir(dir) if !File.exist?(dir)
    File.write(file,contents)

    key = EZQ.send_file_to_s3(file,@bucket,file,compress: compress)
    if compress
      assert_equal "#{file}.gz", key, @wrong_key
    else
      assert_equal file, key, @wrong_key
    end

    clean_file(file)
    refute File.exist?(file), 'File was not removed in cleanup step'
    EZQ.get_s3_file(@bucket,key,decompress: true)
    assert File.exist?(file), @no_file
    assert_equal contents, File.read(file), @wrong_contents
  ensure
    clean_file(file)
    clean_file("#{file}.gz")
  end


  def simple_send(compress)
    file = 'EZQ_unit_tests/file_test.txt'
    dir = 'EZQ_unit_tests'
    send_file(file,dir,file,compress)
  end

  def test_send_file_to_s3
    simple_send(false)
  end


  def test_send_file_to_s3_compressed
    simple_send(true)
  end


  def test_send_file_to_s3_compressed_implicit
    file = 'EZQ_unit_tests/file_test_implicit.txt'
    dir = 'EZQ_unit_tests'
    send_file(file,dir,"#{file}.gz",false)
  end


  def test_md5file
    file = 'test_file.txt'
    clean_file(file)
    File.write(file,"Test")
    known_md5 = "0cbc6611f5540bd0809a388dc95a615b"
    assert_equal known_md5, EZQ.md5file(file).hexdigest, 'MD5s do not match'
  ensure
    clean_file(file)
  end


  def test_get_content_type
    file = 'test.json'
    clean_file(file)
    File.write(file, {"this" => "is a test"}.to_json)
    assert_equal "application/json", EZQ.get_content_type(file), 'Content-type is incorrect'
  ensure
    clean_file(file)
  end


  def test_get_existing_file
    file = 'EZQ_unit_tests/file_test.txt'
    dir = 'EZQ_unit_tests'

    FileUtils.mkdir(dir) if !File.exist?(dir)
    File.write(file,'Some test text')

    key = EZQ.send_file_to_s3(file,@bucket,file)
    assert_equal file, key, @wrong_key

    # Compare modified time to check whether file was pulled down again.
    mtime = File.mtime(file)
    res = EZQ.get_s3_file(@bucket,file)
    assert_equal true, res, 'Failed to get file'
    assert_equal mtime, File.mtime(file), 'File timestamps do not match; file was fetched a second time'
  ensure
    clean_file(file)
  end

end
