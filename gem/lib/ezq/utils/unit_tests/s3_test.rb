require 'minitest/autorun'
require 'logger'
require 'fileutils'
require 'json'
require_relative '../s3.rb'


class TestEZQS3 < Minitest::Test

  def setup
    @bucket = 'test.agsolver'
#     @log = Logger.new(STDOUT)
#     @log.level = Logger::DEBUG
#     EZQ.set_logger(@log)
  end

  def clean_file(file)
    File.unlink(file) if File.exist?(file)
  end

  def test_get_s3
    refute_equal nil, EZQ.get_s3
  end


  def test_send_data_to_s3
    file = 'EZQ_unit_tests/test.txt'
    EZQ.remove_s3_file(@bucket,file)

    EZQ.send_data_to_s3("Test data", @bucket, file)
    EZQ.get_s3_file(@bucket, file)
    assert File.exist?(file)
    assert_equal 'Test data', File.read(file)
  ensure
    FileUtils.rm_rf(file)
  end


  def test_send_data_to_s3_compressed
    file = 'EZQ_unit_tests/compress.txt'
    EZQ.remove_s3_file(@bucket,file)

    EZQ.send_data_to_s3("Test data", @bucket, file, compress: true)
    EZQ.get_s3_file(@bucket, "#{file}.gz", decompress: true)
    assert File.exist?(file)
    assert_equal 'Test data', File.read(file)
  ensure
    clean_file(file)
    clean_file("#{file}.gz")
  end


  def test_send_file_to_s3
    file = 'EZQ_unit_tests/file_test.txt'
    dir = 'EZQ_unit_tests'
    EZQ.remove_s3_file(@bucket,file)

    FileUtils.mkdir(dir) if !File.exist?(dir)
    File.write(file,'Some test text')

    name = EZQ.send_file_to_s3(file,@bucket,file)
    assert_equal file, name

    clean_file(file)
    refute File.exist?(file)
    EZQ.get_s3_file(@bucket,file)
    assert File.exist?(file)
    assert_equal 'Some test text', File.read(file)
  ensure
    clean_file(file)
  end


  def test_send_file_to_s3_compressed
    file = 'EZQ_unit_tests/file_test.txt'
    dir = 'EZQ_unit_tests'
    EZQ.remove_s3_file(@bucket,file)

    FileUtils.mkdir(dir) if !File.exist?(dir)
    File.write(file,'Some test text')

    name = EZQ.send_file_to_s3(file,@bucket,file,compress: true)
    assert_equal "#{file}.gz", name

    clean_file(file)
    refute File.exist?(file)
    EZQ.get_s3_file(@bucket,"#{file}.gz",decompress: true)
    assert File.exist?(file)
    assert_equal 'Some test text', File.read(file)
  ensure
    clean_file(file)
    clean_file("#{file}.gz")
  end


  def test_send_file_to_s3_compressed_implicit
    file = 'EZQ_unit_tests/file_test_implicit.txt'
    dir = 'EZQ_unit_tests'
    EZQ.remove_s3_file(@bucket,file)

    FileUtils.mkdir(dir) if !File.exist?(dir)
    File.write(file,'Some test text')

    name = EZQ.send_file_to_s3(file,@bucket,"#{file}.gz")
    assert_equal "#{file}.gz", name

    clean_file(file)
    refute File.exist?(file)
    EZQ.get_s3_file(@bucket,"#{file}.gz",decompress: true)
    assert File.exist?(file)
    assert_equal 'Some test text', File.read(file)
  ensure
    clean_file(file)
    clean_file("#{file}.gz")
  end

  def test_send_bcf_to_s3
  end


  def test_md5file
    file = 'test_file.txt'
    clean_file(file)
    File.write(file,"Test")
    known_md5 = "0cbc6611f5540bd0809a388dc95a615b"
    assert_equal known_md5, EZQ.md5file(file).hexdigest
  ensure
    clean_file(file)
  end


  def test_get_content_type
    file = 'test.json'
    clean_file(file)
    File.write(file, {"this" => "is a test"}.to_json)
    assert_equal "application/json", EZQ.get_content_type(file)
  ensure
    clean_file(file)
  end


  def test_get_existing_file
    file = 'EZQ_unit_tests/file_test.txt'
    dir = 'EZQ_unit_tests'

    FileUtils.mkdir(dir) if !File.exist?(dir)
    File.write(file,'Some test text')

    name = EZQ.send_file_to_s3(file,@bucket,file)
    assert_equal file, name

    # Compare modified time to check whether file was pulled down again.
    mtime = File.mtime(file)
    res = EZQ.get_s3_file(@bucket,file)
    assert_equal true, res
    assert_equal mtime, File.mtime(file)
  ensure
    clean_file(file)
  end

end
