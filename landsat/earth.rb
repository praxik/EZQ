
# USGS server, LandsatLook, has an index of the available Landsat scene files
# that the EarthExplorer site lets you download. LandsatLook has a proper
# REST API to their ArcGIS server for querying the index.
# Here's some info on one of the methods:
# http://landsatlook.usgs.gov/arcgis/sdk/rest/index.html#/Query_Image_Service/02ss000000m0000000/
#
# http://landsatlook.usgs.gov/arcgis/rest/services/LandsatLook/ImageServer/query?f=json&where=(acquisitionDate >= date'2010-01-01' AND acquisitionDate <= date'2014-12-31') AND (dayOfYear >=1 AND dayOfYear <= 366) AND (sensor = 'TM' OR sensor = 'ETM' OR sensor = 'LANDSAT_ETM' OR sensor = 'OLI') AND (cloudCover <= 20)&returnGeometry=true&spatialRel=esriSpatialRelIntersects&geometry={"xmin":-10428354.855903627,"ymin":5160179.629639412,"xmax":-10391665.082326794,"ymax":5179078.684882897,"spatialReference":{"wkid":102100}}&geometryType=esriGeometryEnvelope&inSR=102100&outFields=sceneID,sensor,acquisitionDate,dateUpdated,path,row,PR,cloudCover,sunElevation,sunAzimuth,receivingStation,sceneStartTime,month,year,OBJECTID,dayOfYear,dayOrNight,browseURL&orderByFields=dayOfYear&outSR=102100

require 'net/http'
require 'json'
require_relative './landsatlook_date_converter'
require_relative './link_scraper'
require_relative './scene_getter'


# Bring in to_datetime method so we don't have to
# qualify with long namespace each time.
include LandsatLookDateConverter

#
# get json from the url
#
def get_json( start_date = '2014-10-01',
              end_date = '2014-10-31',
              cloud_cover = 20,
              xmin = 0,
              xmax = 0,
              ymin = 0,
              ymax = 0,
              sr = 4326
            )

	uri = URI("http://landsatlook.usgs.gov/arcgis/rest/services/LandsatLook/ImageServer/query")

  params = {
    :f => 'json',
    :where => "(acquisitionDate >= date'#{start_date}' AND acquisitionDate <= date'#{end_date}')
              AND (dayOfYear >=1 AND dayOfYear <= 366)
              AND (sensor = 'TM' OR sensor = 'ETM' OR sensor = 'LANDSAT_ETM' OR sensor = 'OLI')
              AND (cloudCover <= #{cloud_cover})",
    :returnGeometry => 'true',
    :spatialRel => 'esriSpatialRelIntersects',
    :geometry => {"xmin"=>xmin,"ymin"=>ymin,"xmax"=>xmax,"ymax"=>ymax,"spatialReference"=>{"wkid"=>sr}}.to_json,
    :geometryType => 'esriGeometryEnvelope',
    :inSR => sr,
    :outFields => 'sceneID,sensor,acquisitionDate,dateUpdated,path,row,PR,cloudCover,
        sunElevation,sunAzimuth,receivingStation,sceneStartTime,month,year,
        OBJECTID,dayOfYear,dayOrNight,browseURL',
    :orderByFields => 'dayOfYear',
    :outSR => sr
	}

	uri.query = URI.encode_www_form(params)

	return Net::HTTP.get_response(uri)
end


#
# parse json data for scene ids, date and cloud cover
#
def get_data(json_str)
	puts 'parsing json string...'
	hash = JSON.parse(json_str)
	puts '...done!'
	puts 'reading data...'

	# loop through all sceneIDs
	features = hash['features']

	features.each do |a|
		data = a['attributes']
    g = a['geometry']
    st_date = to_datetime(data['sceneStartTime'])
    puts "SceneID: #{data['sceneID']}, Cloud Cover: #{data['cloudCover']}, Start Date: #{st_date.to_s}, SR: #{g['spatialReference']}, BU: #{data['browseURL']}"
	end
  return features
end




# test method get_json
puts 'downloading json...'
ref = 4326 #ARGV.shift.to_i

# Voight North
xm=-92.880077
xx=-92.870378
ym=43.269621
yx=43.274183
response = get_json(start_date='2014-01-01',
                    end_date='2014-08-31',
                    cloud_cover=20,
                    xmin=xm,
                    xmax=xx,
                    ymin=ym,
                    ymax=yx,
                    sr=ref)

if response.code != '200'
  warn '...failed, error: ' + response.message
  exit(1)
end

puts '...done!'
features = get_data(response.body)

if features.size == 0
  puts "No scenes for this AOI in this date range."
  exit(0)
end

# Grab the last scene id, and go download that data set as a test
scene_id = features.last['attributes']['sceneID']
puts "Getting data for scene #{scene_id}"
SceneGetter.scene(EELinkScraper.scrape(scene_id),"#{scene_id}.tar.gz")
