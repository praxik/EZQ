require 'nokogiri'
require 'open-uri'


module EELinkScraper

  # Scrapes the url from the last Download button associated
  # with a given scene_id on USGS's earthexplorer website,
  # whose link begins with http://earthexplorer.usgs.gov/download/
  # @param [String] scene_id The scene id
  # @return [String] The download url for this scene id
  def self.scrape(scene_id)
    url = "http://earthexplorer.usgs.gov/download/options/4923/" + scene_id
    page = Nokogiri::HTML(open(url))
    links = page.css("input[type=button][value=Download]").
      select{|u| u['onclick'] =~ /http:\/\/earthexplorer\.usgs\.gov\/download\//}
    res = ''
    if links && !links.empty?
      # url is in the onclick attribute, and has some surrounding junk
      # that must be removed
      res = links.last['onclick'].gsub("window.location='",'')
      res.slice!(-1)
    end
    return res
  end

end


# Test:
# link = EELinkScraper.scrape('LC80260312014079LGN00')
# puts "Link: #{link}"
# puts "Passes: #{link == 'http://earthexplorer.usgs.gov/download/4923/LC80260312014079LGN00/STANDARD/EE'}"
