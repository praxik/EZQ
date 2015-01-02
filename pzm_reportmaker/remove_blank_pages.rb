#!/usr/bin/env ruby

require 'RMagick'
require 'fileutils'

module RemoveBlankPages

# Removes blank pages from a pdf file. "Blankness" is determined by inspecting
# the total number of colors contained in a specified test region of the
# page canvas. If the number of colors equals one, the page is assumed to be
# blank. Notice this will not work if a background other than a solid color
# has been set for the pdf.
# @param filename [String] The pdf file from which to remove blank pages
# @param opts [Hash] Options hash. Available options are
#   * :x [Integer] x offset of test region
#   * :y [Integer] y offset of test region
#   * :width [Integer] width of test region. Set to zero or a negative number
#                      to use remaining width of canvas
#   * :height [Integer] height of test region. Set to zero or a negative number
#                       to use
#   * :pages [Array of Integers] 1-based array of page numbers to test. Leave
#                       empty to test all pages. Notice this is a 1-based array;
#                       that is, page 1 is referenced as 1 rather than as 0.
def self.remove(filename,opts = {:x=>0,:y=>0,:width=>0,:height=>0,:pages=>[]})

  x = opts.has_key?(:x) ? opts[:x] : 0
  x = x < 0 ? 0 : x
  y = opts.has_key?(:y) ? opts[:y] : 0
  y = y < 0 ? 0 : y
  opts[:width] ||= 0
  opts[:height] ||= 0
  opts[:pages] ||= []
  img_list = Magick::Image.read(filename)
  test_pages = opts[:pages].empty? ? (1..img_list.size).to_a : opts[:pages]
  to_remove = []

  test_pages.each do |pg|
    img = img_list[pg - 1] # Convert from 1-based counting to 0-based counting
    # Must figure width and height separately for each page since there's no
    # guarantee each page will have the same dimensions.
    width = opts[:width] <= 0 ? (img.columns - x) : opts[:width]
    height = opts[:height] <= 0 ? (img.rows - y) : opts[:height]
    to_remove << pg if (img.crop(x,y,width,height).number_colors < 2)
  end

  return nil if to_remove.empty?

  puts "Removing pages #{to_remove}"

  to_keep = (1..img_list.size).to_a - to_remove
  puts "Keeping pages #{to_keep}"
  system("pdftk #{filename} cat #{to_keep.join(' ')} output out.pdf")
  FileUtils.mv('out.pdf',filename)
  
  return nil
end

end

################################################################################
# Run this bit if this file is being run directly as an executable rather than
# being imported as a module.
if __FILE__ == $0

if ARGV.count < 1
  warn 'Usage: remove_blank_pages FILE [opts]'
  warn ''
  warn '  opts is a quoted version of a ruby hash supporting the following'
  warn '  keys: :x, :y, :width, :height, :pages'
  exit(1)
end

filename = ARGV.shift
opts = eval(ARGV.shift)

RemoveBlankPages.remove(filename,opts)

exit(0)

end
