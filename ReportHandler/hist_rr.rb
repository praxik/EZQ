require 'nokogiri'

input_file = ARGV.shift

job_id, rec_id, year_id, rr = input_file.split('_')
doc = Nokogiri::XML(open("json/#{input_file}"))
hist = doc.search('HistItem')[1]

#hist.at_xpath('HistMin').to_s

min = hist.at_xpath('HistMin').text.to_f
max = hist.at_xpath('HistMax').text.to_f
bincount = hist.at_xpath('BucketCount').text.to_i
binwidth = (max-min)/bincount
counts = hist.at_xpath('HistCounts').text

counts = counts.split('|')
counts = counts.map{|i| i.to_i * 9 * 0.000247105} # *9 converts from mapunit to
                                                  # m^2. 0.0002... converts from
                                                  # m^2 to acre

hashed = {}
counts.each_with_index{|v,i| hashed[min + i*binwidth] = v }

rebinned_hist = {}
rebinned_hist[-1.0] = hashed.reduce(0){|sum,(k,v)| sum = k < -0.9 ? sum + v : sum}
(-0.8..0.8).step(0.2).each do |binn|
  rebinned_hist[binn] = hashed.reduce(0){|sum,(k,v)| sum = (k >= (binn-0.1) and k < (binn + 0.1)) ? sum + v : sum}
end
rebinned_hist[1.0] = hashed.reduce(0){|sum,(k,v)| sum = k >= 0.9 ? sum + v : sum}

colors = ['ff0000','f72b09','ef5612','e7811b','dfac24','d8d82e',
          'bdce24','a3c51b','89bc12','6fb309','55aa00']

# Convert hex colors to 0..1 RGB with an alpha channel tacked on
alpha = 0.85
colors = colors.map do |hex|
  hex.scan(/../).map{|hp| hp.to_i(16)/255.0} << alpha
end

fname = "report_data/rate_of_return_hist_#{year_id}.svg"
command = "DISPLAY=:0 python histplot_rr.py #{fname} \"#{rebinned_hist.keys.to_s}\" \"#{rebinned_hist.values.to_s}\" \"#{colors.to_s}\""
system(command)
