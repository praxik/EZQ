# All functions in this file either extract information directly
# from a Ruby Hash version of the original JSON input, or use
# that input to run an external program that returns information.
#
# Functions in this file should not produce side-effects that are used
# elsewhere.


module Extractors


def self.set_logger(logger)
  @log = logger
end

# Gets all the files from S3 that we'll need based on refs in the input json.
# @param [Hash] input The input hash containing appropriate file refs.
# @return [Bool] true if all file gets succeeded; false otherwise
def self.get_files(input)
  bucket = 'roi.agsolver'
  keys = Set.new()
  keys << input['geojson_file']
  input['scenarios'].each do |scenario|
    keys << scenario['get_tiff_raster_path']
    keys << scenario['get_yield_json_file_path']
  end
  successes = keys.to_a.map{|key| EZQ.get_s3_file(bucket,key)}
  return successes.reduce(:&)
end



# Run the binary that processes the input json and all the images.
def self.run_binary(input)
  f = "in.json"
  File.unlink(f) if File.exists?(f)
  File.write(f,input.to_json)
  yields = nil
  success,output = EZQ.exec_cmd("LD_LIBRARY_PATH=. SAGA_MLB=./saga ./6k_pzm_report -p #{f} -o cb_images/")
  @log.error output if !success
  if success and output.size > 0
    # Convert array of strings that looks like ["junk\n",
    #                                           "pzm: 132,234.3,192.8\n",
    #                                           "more junk\n",
    #                                           "stuff\n",
    #                                           "pzm: 144,45.323,33.87\n"]
    # into a hash that looks like: {132 => {:avg => 234.3, :nz => 192.8},
    #                               144 => {:avg => 45.323, :nz => 33.87}}
    yields = output.select{|t| t =~ /^pzm/}
    yields = yields.map{|t| t.chomp.gsub('pzm: ','').split(',')}
    yields = yields.map{|a| [a[0].to_i,[[:avg,a[1].to_f],[:nz,a[2].to_f]].to_h]}.to_h
  end
  # Return either a hash or nil
  return yields
end



# Returns a hash in which each key is an input yield raster and the
# associated value is the desired name of the reprojected raster.
# @param [Hash] input Ruby hash containing the full JSON sent by web app
def self.get_yield_raster_hash(input)
  hash = {}
  input['scenarios'].each do |scenario|
    raster_in = scenario['get_tiff_raster_path']
    raster_out = "report/#{scenario['id']}_yield.tiff"
    hash[raster_in] = raster_out
  end
  return hash
end



# Returns a hash containing a cid as the key and map coords as the value.
# One entry in the hash has the key "field", which refers to the
# whole-field coordinates.
def self.get_boundary_hash(input)
  coords = {}
  coords['field'] = input['map_coords_as_geojson']
  input['scenarios'].each do |scenario|
    scenario['management_zones'].each do |mz|
      coords["#{scenario['id']}_#{mz['id']}"] = mz['map_coords_as_geojson']
    end
  end
  return coords
end



def self.get_scenario_tiff_hash(input)
  hash = {}
  input['scenarios'].each do |scenario|
    scenario_id = scenario['id']
    tiff = scenario['get_tiff_raster_path']
    hash[scenario_id] = tiff
  end
  return hash
end



def self.get_cids(input)
  cids = []
  input['scenarios'].each do |scenario|
    cids << "#{scenario['id']}"
    scenario['management_zones'].each do |mz|
      cids << "#{scenario['id']}_#{mz['id']}"
    end
  end
  return cids
end

end # module
