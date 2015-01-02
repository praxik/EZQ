# Functions in this file are used specifically for their side-effects.
# Do not assume return values from these functions are meaningful
# in any way. Many of these functions write files to disk according
# to a pre-determined naming scheme based on inputs.

module SeTransforms

# Reprojects raster_in to EPSG:3857 and saves as raster_out
# @param [String] raster_in Path to input raster
# @param [String] raster_out Path to output raster
def self.reproject_yield_raster(raster_in,raster_out)
  info_cmd = "gdalinfo \"#{raster_in}\" | grep NoData"
  no_data = EZQ.exec_cmd(info_cmd).flatten.last.split('=').last.strip
  cmd = "gdalwarp -r cubicspline" +
        " -t_srs EPSG:3857" +
        " -srcnodata #{no_data}" +
        " -dstnodata #{no_data}" +
        " \"#{raster_in}\"" +
        " \"#{raster_out}\"" +
        " -overwrite"
  #puts cmd
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
  cmd = "ogr2ogr -t_srs EPSG:3857 -f \"GeoJSON\" #{out_name} #{in_name}"
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

  # Convert the shapefile back to geojson.
  EZQ.exec_cmd("ogr2ogr -f \"GeoJSON\" #{collected_name} #{master}.shp")

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
