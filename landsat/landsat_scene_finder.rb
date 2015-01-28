#!/usr/bin/env ruby

require 'net/http'
require 'json'
require_relative './landsatlook_date_converter'
require_relative './ezqlib'

module LsSceneFinder

  # Get an array of scene metadata from LandsatLook.
  # @param [String] start_date Beginning of scene date range
  #                 in yyyy-mm-dd format
  # @param [String] end_date End of scene date range
  #                 in yyyy-mm-dd format
  # @param [Numeric] cloud_cover Maximum allowed percentage cloud cover
  # @param [String] aoi_file Path to geojson file containing
  #                 area of interest
  # @return [Array(Hash)] Array of Hash objects conforming to
  #         {:sceneID => String,
  #          :cloudCover => Integer,
  #          :sceneStartTime => DateTime,
  #          :browseURL => String}
  #          If no valid scenes are returned, the array will
  #          simply be empty.
  def LsSceneFinder.find_scenes( start_date = '2014-10-01',
                                 end_date = '2014-10-31',
                                 cloud_cover = 20,
                                 aoi_file = '' )

    wkid = extract_wkid(aoi_file)
    xmin,ymin,xmax,ymax = get_bbox(aoi_file)
    puts "Bbox = #{xmin}, #{ymin}, #{xmax}, #{ymax}, #{wkid}"
    response = get_json(start_date,end_date,cloud_cover,xmin,xmax,ymin,ymax,wkid)
    raise(response.message) if response.code != '200'
    return extract(response.body)
  end



  private


  def LsSceneFinder.extract_wkid(file)
    j = JSON.parse(File.read(file))
    crs = j.fetch('crs',nil)
    return 4326 if !crs
    name = crs["properties"]["name"]
    return name.split(':').last.to_i
  end



  def LsSceneFinder.get_bbox(file)
    # Ensure we use out internally packaged version of ogrinfo
    cmd = "./ogrinfo -al -so -ro #{file} | grep Extent"
    extent = EZQ.exec_cmd(cmd).last.first
    puts extent

    # extent contains a string like this:
    # Extent: (-92.880077, 43.269621) - (-92.870378, 43.274183)

    extent.gsub!('Extent: ','')
    extent.slice!(0)
    extent.slice!(extent.length - 1)
    pairs = extent.split(') - (')
    return pairs.map{|p| p.split(', ')}.flatten.map{|c| c.to_f}
  end



  # Get scene metadata from LandsatLook server
  def LsSceneFinder.get_json(start_date,end_date,cloud_cover,xmin,xmax,ymin,ymax,wkid)

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
    puts "\nUsage: landsat_scene_finder [START_DATE]
          [END_DATE] [CLOUD_COVER] [AOI_GEOJSON_FILE]

          START DATE and END DATE should be in form yyyy-mm-dd
          CLOUD_COVER should be an integer"
    exit
  end

  vars = []
  vars = ARGV


  sd = vars[0] ? vars[0] : '2014-01-01'
  ed = vars[1] ? vars[1] : '2014-08-31'
  cc = vars[2] ? vars[2] :  20

  aoi = vars[3] ? vars[3] : 'test_aoi.geojson'

  # Voight North
#   xm = vars[3] ? vars[3] : -92.880077
#   xx = vars[4] ? vars[4] : -92.870378
#   ym = vars[5] ? vars[5] : 43.269621
#   yx = vars[6] ? vars[6] : 43.274183
#   wk = vars[7] ? vars[7] : 4326

  data = LsSceneFinder.find_scenes( start_date = sd,
                                    end_date = ed,
                                    cloud_cover = cc.to_i,
                                    aoi )

  puts data
end

