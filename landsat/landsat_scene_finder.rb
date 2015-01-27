#!/usr/bin/env ruby

# Original set of outFields:
#  'sceneID,sensor,acquisitionDate,dateUpdated,path,row,PR,cloudCover,
#   sunElevation,sunAzimuth,receivingStation,sceneStartTime,month,year,
#   OBJECTID,dayOfYear,dayOrNight,browseURL'

require 'net/http'
require 'json'
require_relative './landsatlook_date_converter'

module LsSceneFinder

  # Get an array of scene metadata from LandsatLook.
  # @param [String] start_date Beginning of scene date range
  #                 in yyyy-mm-dd format
  # @param [String] end_date End of scene date range
  #                 in yyyy-mm-dd format
  # @param [Numeric] Maximum allowed percentage cloud cover
  # @param [Numeric] xmin Bounding box minumum x coordinate
  # @param [Numeric] xmax Bounding box maximum x coordinate
  # @param [Numeric] ymin Bounding box minumum y coordinate
  # @param [Numeric] ymax Bounding box maximum y coordinate
  # @param [Integer] wkid WKID of the input spatial representation
  # @return [Array(Hash)] Array of Hash objects conforming to
  #         {:sceneID => String,
  #          :cloudCover => Integer,
  #          :sceneStartTime => DateTime,
  #          :browseURL => String}
  #          If no valid scenes are returned, the array will
  #          simply be empty.
  def LsSceneFinder.find_scenes(
                                start_date = '2014-10-01',
                                end_date = '2014-10-31',
                                cloud_cover = 20,
                                xmin = 0,
                                xmax = 0,
                                ymin = 0,
                                ymax = 0,
                                wkid = 4326 )

    response = get_json(start_date,end_date,cloud_cover,xmin,xmax,ymin,ymax,wkid)

    raise(response.message) if response.code != '200'

    return extract(response.body)
  end



  private

  # Get scene metadata from LandsatLook server
  def LsSceneFinder.get_json( start_date,end_date,cloud_cover,xmin,xmax,ymin,ymax,wkid)

    uri = URI("http://landsatlook.usgs.gov/arcgis/rest/services/LandsatLook/ImageServer/query")

    params = {
      :f => 'json',
      :where => "(acquisitionDate >= date'#{start_date}'
                AND acquisitionDate <= date'#{end_date}')
                AND (dayOfYear >=1 AND dayOfYear <= 366)
                AND (sensor = 'TM' OR sensor = 'ETM'
                    OR sensor = 'LANDSAT_ETM' OR sensor = 'OLI')
                AND (cloudCover <= #{cloud_cover})",
      :returnGeometry => 'true',
      :spatialRel => 'esriSpatialRelIntersects',
      :geometry => {"xmin"=>xmin,"ymin"=>ymin,
                    "xmax"=>xmax,"ymax"=>ymax,
                    "spatialReference"=>{"wkid"=>wkid}}.to_json,
      :geometryType => 'esriGeometryEnvelope',
      :inSR => wkid,
      :outFields => 'sceneID,cloudCover,sceneStartTime,dayOfYear,browseURL',
      :orderByFields => 'dayOfYear',
      :outSR => wkid
    }

    uri.query = URI.encode_www_form(params)

    return Net::HTTP.get_response(uri)
  end



  # Extract just the info we need from the JSON input and convert the
  # goofy timestamps to DateTime objects
  def LsSceneFinder.extract(json_str)
    hash = JSON.parse(json_str)
    metadata = []

    features = hash['features']

    metadata = features.map do |f|
      a = f['attributes']
      st_date = LsDate.to_datetime(a['sceneStartTime'])
      # Emit this:
      {:sceneID => a['sceneID'],
       :cloudCover => a['cloudCover'],
       :sceneStartTime => st_date,
       :browseURL => a['browseURL']}
    end

    return metadata
  end

end # module





# Execute this if run as standalone application
if __FILE__ == $0

  if ['-h','--help'].include?(ARGV[0])
    puts "\nUsage: landsat_scene_finder [-f FILE] [START_DATE]
          [END_DATE] [CLOUD_COVER] [XMIN] [XMAX]
          [YMIN] [YMAX] [WKID]

          Seriously though, do yourself a favor and shove all the
          positional arguments into a file and invoke this thing
          like so:

          landsat_scene_finder -f arg_file\n\n"
    exit
  end

  vars = []

  if ARGV[0] == '-f'
    v = File.readlines(ARGV[1]).first
    vars = v.split(' ')
  else
     vars = ARGV
  end


  sd = vars[0] ? vars[0] : '2014-01-01'
  ed = vars[1] ? vars[1] : '2014-08-31'
  cc = vars[2] ? vars[2] :  20

  # Voight North
  xm = vars[3] ? vars[3] : -92.880077
  xx = vars[4] ? vars[4] : -92.870378
  ym = vars[5] ? vars[5] : 43.269621
  yx = vars[6] ? vars[6] : 43.274183
  wk = vars[7] ? vars[7] : 4326

  data = LsSceneFinder.find_scenes( start_date = sd,
                                    end_date = ed,
                                    cloud_cover = cc.to_i,
                                    xmin = xm.to_f,
                                    xmax = xx.to_f,
                                    ymin = ym.to_f,
                                    ymax = yx.to_f,
                                    wkid = wk.to_i )

  puts data
end

