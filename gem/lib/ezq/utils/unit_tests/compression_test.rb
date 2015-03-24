require 'fileutils'
require 'zlib'
require 'minitest/autorun'
require_relative '../compression.rb'


class TestEZQCompression < Minitest::Test


  def clean_file(file)
    File.unlink(file) if File.exist?(file)
  end


  def test_compress_file
    file = 'test_file'
    zfile = "#{file}.gz"

    clean_file(file)
    clean_file(zfile)
    File.write(file,"Test data")

    EZQ.compress_file('test_file')
    assert File.exist?(zfile)
  ensure
    clean_file(zfile)
  end


  def test_gunzip
    file = 'test_file'
    zfile = "#{file}.gz"
    File.write(file,"Test data")
    EZQ.compress_file('test_file')
    clean_file(file)

    EZQ.gunzip(zfile)
    assert File.exist?(file)
    assert_equal 'Test data', File.read(file)
  ensure
    clean_file(zfile)
    clean_file(file)
  end


  def test_decompress_file
    file = 'test_zip.zip'

    clean_file(file)
    File.write(file,ZFILE)

    EZQ.decompress_file(file)
    assert File.exist?('zipped')
    assert File.directory?('zipped')
    assert_equal "One\n", File.read('zipped/one.txt')

  ensure
    FileUtils.rm_rf('zipped')
    clean_file(file)
  end


  def test_decompress_headerless_file
    file = 'headerless.txt'

    File.write(file,Zlib::Deflate.deflate('Test text.'))
    refute_equal "Test text.", File.read(file)

    EZQ.decompress_headerless_file(file)
    assert_equal "Test text.", File.read(file)
  ensure
    clean_file(file)
  end


  ZFILE =
"PK\u0003\u0004\n\u0000\u0000\u0000\u0000\u0000\x8C\x84aF\u0000\u0000\u0000"+
"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\a\u0000\u001C"+
"\u0000zipped/UT\t\u0000\u0003g\x94\xF3Th\x94\xF3Tux\v\u0000\u0001\u0004\xE8"+
"\u0003\u0000\u0000\u0004\xE8\u0003\u0000\u0000PK\u0003\u0004\n\u0000\u0000"+
"\u0000\u0000\u0000\x8C\x84aFJ\xA7%6\u0004\u0000\u0000\u0000\u0004\u0000\u0000"+
"\u0000\u000E\u0000\u001C\u0000zipped/two.txtUT\t\u0000\u0003g\x94\xF3Tg\x94"+
"\xF3Tux\v\u0000\u0001\u0004\xE8\u0003\u0000\u0000\u0004\xE8\u0003\u0000"+
"\u0000Two\nPK\u0003\u0004\n\u0000\u0000\u0000\u0000\u0000\x88\x84aF\xA1\a%X"+
"\u0004\u0000\u0000\u0000\u0004\u0000\u0000\u0000\u000E\u0000\u001C"+
"\u0000zipped/one.txtUT\t\u0000\u0003_\x94\xF3T_\x94\xF3Tux\v\u0000\u0001"+
"\u0004\xE8\u0003\u0000\u0000\u0004\xE8\u0003\u0000\u0000One\nPK\u0001\u0002"+
"\u001E\u0003\n\u0000\u0000\u0000\u0000\u0000\x8C\x84aF\u0000\u0000\u0000\u0000"+
"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\a\u0000\u0018\u0000\u0000"+
"\u0000\u0000\u0000\u0000\u0000\u0010\u0000\xFDA\u0000\u0000\u0000"+
"\u0000zipped/UT\u0005\u0000\u0003g\x94\xF3Tux\v\u0000\u0001\u0004\xE8\u0003"+
"\u0000\u0000\u0004\xE8\u0003\u0000\u0000PK\u0001\u0002\u001E\u0003\n\u0000"+
"\u0000\u0000\u0000\u0000\x8C\x84aFJ\xA7%6\u0004\u0000\u0000\u0000\u0004\u0000"+
"\u0000\u0000\u000E\u0000\u0018\u0000\u0000\u0000\u0000\u0000\u0001\u0000\u0000"+
"\u0000\xB4\x81A\u0000\u0000\u0000zipped/two.txtUT\u0005\u0000\u0003g\x94"+
"\xF3Tux\v\u0000\u0001\u0004\xE8\u0003\u0000\u0000\u0004\xE8\u0003\u0000"+
"\u0000PK\u0001\u0002\u001E\u0003\n\u0000\u0000\u0000\u0000\u0000\x88\x84aF"+
"\xA1\a%X\u0004\u0000\u0000\u0000\u0004\u0000\u0000\u0000\u000E\u0000\u0018"+
"\u0000\u0000\u0000\u0000\u0000\u0001\u0000\u0000\u0000\xB4\x81\x8D\u0000\u0000"+
"\u0000zipped/one.txtUT\u0005\u0000\u0003_\x94\xF3Tux\v\u0000\u0001\u0004\xE8"+
"\u0003\u0000\u0000\u0004\xE8\u0003\u0000\u0000PK\u0005\u0006\u0000\u0000\u0000"+
"\u0000\u0003\u0000\u0003\u0000\xF5\u0000\u0000\u0000\xD9\u0000\u0000\u0000\u0000"+
"\u0000"

end
