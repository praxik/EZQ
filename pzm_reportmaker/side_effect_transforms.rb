# Functions in this file are used specifically for their side-effects.
# Do not assume return values from these functions are meaningful
# in any way. Many of these functions write files to disk according
# to a pre-determined naming scheme based on inputs.

module SeTransforms

  def self.set_logger(logger)
    @log = logger
  end

# Reprojects raster_in to EPSG:3857 and saves as raster_out
# @param [String] raster_in Path to input raster
# @param [String] raster_out Path to output raster
def self.reproject_raster(raster_in,raster_out)
  info_cmd = "gdalinfo \"#{raster_in}\" | grep NoData"
  no_data = EZQ.exec_cmd(info_cmd).flatten.last.split('=').last.strip
  cmd = "gdalwarp -r cubicspline" +
        " -t_srs EPSG:3857" +
        " -srcnodata #{no_data}" +
        " -dstnodata #{no_data}" +
        " -overwrite" +
        " \"#{raster_in}\"" +
        " \"#{raster_out}\""
  @log.debug "reproject #{raster_in} #{raster_out}" if @log
  EZQ.exec_cmd(cmd)
end



# Reprojects a hash of geojson boundaries to EPSG:3857 and stores in a file.
# Side-effects only; no useful return value.
# @param [String] out_name Path to file that should hold the reprojected bounds
# @param [String] coords GeoJSON string of coordinates to reproject
# @return nil
def self.reproject_boundaries(out_name,coords)
  in_name = "#{out_name}.tmp.geojson"
  File.write(in_name,coords)
  # Overwrite flag in call to ogr2ogr does not cause file to be overwritten
  File.unlink(out_name) if File.exist?(out_name)
  cmd = "ogr2ogr -t_srs EPSG:3857 -f \"GeoJSON\" -overwrite #{out_name} #{in_name}"
  EZQ.exec_cmd(cmd)
  return nil
end



def self.collect_coords(master,pieces_array,collected_name)
  # Convert field boundary to shapefile
  File.unlink("#{master}.shp") if File.exist?("#{master}.shp")
  EZQ.exec_cmd("ogr2ogr -f \"ESRI Shapefile\" #{master}.shp #{master}")

  # Append each zone boundary to the field shapefile.
  pieces_array.each do |f|
    EZQ.exec_cmd("ogr2ogr -update -append -f \"ESRI Shapefile\" #{master}.shp #{f}")
  end

  # Replace the prj file with one containing correct projection info
  prj = <<-END.gsub(/^  /,'').split("\n").join()
  PROJCS["WGS 84 / Pseudo-Mercator",GEOGCS["WGS 84",DATUM["WGS_1984",
  SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],
  AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],
  UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],
  AUTHORITY["EPSG","4326"]],PROJECTION["Mercator_1SP"],
  PARAMETER["central_meridian",0],PARAMETER["scale_factor",1],
  PARAMETER["false_easting",0],PARAMETER["false_northing",0],
  UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["X",EAST],
  AXIS["Y",NORTH],EXTENSION["PROJ4","+proj=merc +a=6378137 +b=6378137
   +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m
   +nadgrids=@null +wktext  +no_defs"],AUTHORITY["EPSG","3857"]]
  END
  File.write("#{master}.prj",prj)

  # Convert the shapefile back to geojson.
  File.unlink(collected_name) if File.exist?(collected_name)
  res = EZQ.exec_cmd("ogr2ogr -t_srs EPSG:3857 -f \"GeoJSON\" -overwrite #{collected_name} #{master}.shp")
  @log.error(res.last) if !res.first && @log

  return nil
end



def self.make_expenses_pie_chart(budget_items,out_file)
  sb = budget_items
  values = sb.map{|bi| bi['amount']}
  labels = sb.map{|bi| bi['item_name']}
  labels = labels.map{|t| t.gsub(/\//,"/\n")}
  return EZQ.exec_cmd(['python', 'pie_chart.py',out_file,labels.to_s,values.to_s])
end

end # module
