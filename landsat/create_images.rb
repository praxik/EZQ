require_relative './leaf_wrapper_gis_landsat_ruby'


module NDVI

  include Leaf_wrapper_gis_landsat_ruby


  # Creates an NDVI of an Area of Interest (aoi) from a LandsatLook
  # scene.
  # @param [String] scene_root Path to directory containing
  #                 scene data
  # @param [String] scene_id ID of the scene
  # @param [String] aoi_file Path to geojson file containing aoi
  #                 boundaries. Does not need to be bounding box only.
  def NDVI.from_landsat( scene_root, scene_id, aoi_file )

    @err_message = ''

    if !Dir.exist?(scene_root)
      @err_message = "Directory '#{scene_root}' does not exist or is inaccessible"
      return false
    end

    if !File.exist?(aoi_file)
      @err_message = "AOI file '#{aoi_file}' does not exist or is inaccessible"
      return false
    end

    CreateNDVIFromLandsat( scene_root, scene_id, aoi_file );

    # Check for existence of output image...
    return true

  end



  # Returns the last error encountered since most recent call
  # to +from_landsat+
  # @return [String]
  def NDVI.last_error
    return @err_message || ''
  end

end




# Test
# Execute this if run as standalone application
if __FILE__ == $0

  a = '/home/ubuntu/dev/deps/LC81050112014233LGN00';
  b = 'LC81050112014233LGN00';
  c = 'field_Voight_10_1415115678.geojson';

  puts NDVI.last_error if !NDVI.from_landsat(a,b,c)

end
