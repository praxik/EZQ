require 'nokogiri'

input_file = ARGV.shift
output_file = ARGV.shift

doc = Nokogiri::XML(open(input_file))
hist = doc.search('Histograms').first.at_xpath('HistItem')

#hist.at_xpath('HistMin').to_s

min = hist.at_xpath('HistMin').text.to_f
max = hist.at_xpath('HistMax').text.to_f
bincount = hist.at_xpath('BucketCount').text.to_i
binwidth = (max-min) / bincount
counts = hist.at_xpath('HistCounts').text

counts = counts.split('|')
counts = counts.map{|i| i.to_i * 9 * 0.000247105} # *9 converts from mapunit to
                                                  # m^2. 0.0002... converts from
                                                  # m^2 to acre

hashed = {}
counts.each_with_index{|v,i| hashed[min + i*binwidth] = v }

rebinned_hist = {}
rebinned_hist[-500] = hashed.reduce(0){|sum,(k,v)| sum = k < -450 ? sum + v : sum}
(-400..400).step(100).each do |binn|
  rebinned_hist[binn] = hashed.reduce(0){|sum,(k,v)| sum = (k >= (binn-50) and k < (binn + 50)) ? sum + v : sum}
end
rebinned_hist[500] = hashed.reduce(0){|sum,(k,v)| sum = k >= 450 ? sum + v : sum}

colors = ['ff0000','f72b09','ef5612','e7811b','dfac24','d8d82e',
          'bdce24','a3c51b','89bc12','6fb309','55aa00']

# Convert hex colors to 0..1 RGB with an alpha channel tacked on
alpha = 0.85
colors = colors.map do |hex|
  hex.scan(/../).map{|hp| hp.to_i(16)/255.0} << alpha
end


# DISPLAY=:0
command = "python histplot.py #{output_file} \"#{rebinned_hist.keys.to_s}\" \"#{rebinned_hist.values.to_s}\" \"#{colors.to_s}\""
system(command)
