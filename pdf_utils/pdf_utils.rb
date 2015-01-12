#!/usr/bin/env ruby

require 'logger'
require 'pdfkit'
require 'erb'
require_relative './ezqlib'
require_relative './number_formatters'


module AgPdfUtils

  # Give templates access to number formatters without having to
  # qualify the module name at every invocation.
  include NumberFormatters



  # Set a logger target for PdfUtil module so these functions can
  # have someone to talk to.
  # @param [Logger] logger A Ruby Logger.
  # @return [Logger] The same logger you passed in, allowing you to chain
  #                   this with other log setter functions.
  def self.set_logger(logger)
    @log = logger
    return logger
  end


  # Convert an html file to a pdf file of the same name with .pdf tacked
  # on to the end.
  # @param [String] html Path to input html
  # @param [String] header Path, relative to html file location, of an
  #                        html file containing a header that will be
  #                        applied to every page in the pdf. Defaults to
  #                        +header.html+
  # @return [String] Name of generated pdf.
  def self.html_to_pdf(html,header='header.html')
    # We must cd into the dir containing the html to allow relative file
    # references in the html to work as expected
    pwd = Dir.pwd()
    Dir.chdir(File.dirname(File.absolute_path(html)))
    html_in = File.basename(html)

    # These are generally good margins for printing to Letter size paper
    pdfkit = PDFKit.new(File.new("#{html_in}"),
                      :page_size => 'Letter',
                      :margin_left => '2mm',
                      :margin_right => '2mm',
                      :margin_top => '35mm',
                      :margin_bottom => '10mm',
                      :header_html => header
                      )

    pdf_file = "#{html_in}.pdf"
    pdfkit.to_file(pdf_file)
    # Undo the chdir from above
    Dir.chdir(pwd)
    # Name of generated pdf
    return "#{html}.pdf"
  end



  # Run ERB to expand template in_file and write the html result to out_file
  # @param [Any] d Data structure to which ERB will have access via local
  #                binding.
  # @param [String] in_file Path to input .erb file, eg.
  #                         +"templates/cool.html.erb"+
  # @param [String] out_file Path to ouput html file, eg.
  #                         +"output/cool_stuff.html"+
  # @return [String] The value passed in as parameter +out_file+
  def self.generate_html(d,in_file,out_file)
    @log.info "Generating #{out_file}" if @log
    out_file = File.absolute_path(out_file)
    pwd = Dir.pwd()
    # Changing into in_file's directory ensures that all relative
    # paths mentioned in in_file work properly
    Dir.chdir(File.dirname(in_file))
    erbed = ERB.new(File.read(File.basename(in_file)))

    File.write(out_file,erbed.result(binding))
    Dir.chdir(pwd)

    return out_file
  end



  # Combine all files referenced in in_files into a single pdf named
  # output_name
  # @param in_files [Array(String)] Files to stitch together
  # @param output_name [String] Name to use for stitched file
  # @return [Bool] True if successful; false otherwise
  def self.stitch(in_files,output_name)
    res = EZQ.exec_cmd("gs -dBATCH -dNOPAUSE -q -sDEVICE=pdfwrite -dPDFSETTINGS=/prepress -sOutputFile=#{output_name_name} #{in_files.join(' ')}")
    @log.error "stitch error: #{res.last}" if !res.first
    return res.first
  end



  # Return the number of pages in a pdf
  # @param [String] filename PDF to examine
  # @return [Integer] Number of pages in the PDF. Returns 0 if PDF
  #                    does not exist.
  def self.get_num_pages(filename)
    if File.exist?(filename)
      res = EZQ.exec_cmd("pdftk #{filename} dump_data")
      if res.first
        return res.last.select{|t| t =~ /^NumberOfPages/}.first.split(':').last.
                    strip.to_i
      end
    end
    return 0
  end

end # module
