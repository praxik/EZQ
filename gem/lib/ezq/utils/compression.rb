require 'zip'
require 'zlib'
require 'securerandom'

require 'ezq/utils/common'


module EZQ

  # Decompress a .zip archive
  #
  # @param [String] filename Path to zip archive
  #
  # @param [Boolean] overwrite Whether to overwrite existing files when
  #   decompressing. Default: true.
  #
  # @return nil
  def EZQ.decompress_file(filename, overwrite: true)
    Zip.on_exists_proc = true # Don't raise if extracted files already exist
    Zip::File.open(filename) do |zip_file|
      zip_file.each do |entry|
        FileUtils.mkdir_p(File.dirname(entry.name))
        entry.extract(entry.name) if (overwrite || !File.exists?(entry.name))
      end
    end
    return nil
  end


  # Decompress a file that contains data compressed directly with libz; that
  # is, the file is not a standard .zip with appropriate header information.
  # Decompresses the file and stores the result in a file with the same name.
  #
  # @param [String] filename Path to compressed file
  #
  # @return [String] Path to uncompressed file
  def EZQ.decompress_headerless_file(filename)
    uncname = "#{Dir.tmpdir}/#{filename}.uc"
    File.open(filename) do |cf|
      zi = Zlib::Inflate.new(Zlib::MAX_WBITS + 32)
      File.open(uncname, "w+") {|ucf| ucf << zi.inflate(cf.read) }
      zi.close
    end
    File.delete(filename)
    File.rename(uncname, filename)
    return filename
  end


  # Decompress a .gz file
  #
  # @param [String] filename Path to gz file
  #
  # @param [Boolean] keep_name If true, uncompressed file will
  #   retain the .gz extension. Default: false.
  #
  # @return [String] Path to the decompressed file
  def EZQ.gunzip(filename,keep_name: false)
    @log.debug "EZQ::gunzip: #{filename}, #{keep_name}" if @log
    uncname = ''
    File.open(filename) do |cf|
      zi = Zlib::Inflate.new(Zlib::MAX_WBITS + 32)
      # Strip .gz from the end of the filename
      uncname = "#{Dir.tmpdir}/#{SecureRandom.hex(8)}.unc"
      @log.debug "EZQ::gunzip: decompressing to #{uncname}" if @log
      File.open(uncname, "w+") {|ucf| ucf << zi.inflate(cf.read) }
      zi.close
    end

    if keep_name
      File.delete(filename)
      File.rename(uncname,filename)
      return filename
    else
      newname = filename.gsub(/\.gz$/,'')
      File.rename(uncname, newname)
      return newname
    end
  end


  # Compresses the file and stores the result in filename.gz
  #
  # @param [String] filename Path to file to compress
  #
  # @return [String] Path to the compressed file
  def EZQ.compress_file(filename)
    cfname = "#{filename}.gz"
    Zlib::GzipWriter.open(cfname,9) do |gz|
      gz.mtime = File.mtime(filename)
      gz.orig_name = filename
      gz.write IO.binread(filename)
    end
    return cfname
  end

end
