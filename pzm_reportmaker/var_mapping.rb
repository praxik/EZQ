#!/usr/bin/env ruby

# Download the ref'd files all through except the get_yield_map_path refd here:
# yield": {
#               "id": 109,
#               "get_yield_map_path": "/yields/yield_maps/000/000/109/original/Jellum_2013.zip"

# Pull out the map_coords_as_geojson element for each MZ, and write each to
# MZ_id.geojson (id tag in each MZ is an integer) and dump it in same dir
# as full field geojson, which refd in top-level tag geojson_file
# So, you'll have as many of these files as there are management zones.

# Pass the entire input doc that we're parsing here into the C++ app.


# unit_id = 1 is $/ac
# unit_id = 2 is $/bu

# C++ app will return processed images that can be input to qgis,
# along with json doc or docs detailing yield info that I can combine with
# Commodity price to get Revenue for a field/zone.

# Question: when do I want to do the qgis stage:
#  1. All at once
#  2. Just before each image is needed
#
# Best to figure out how I want report workflow to go.
# 1. Each section will be generated by a separate function
# 2. Sections that are generated multiple times will take care of own iteration,
#    but will essentially use an outer loop that iterates over the bulk of the
#    function body so that variables passed into the erb keep the same names
#    and the erb doesn't have to reference an index into a sub-hash each time.
# 3. Given these two, it makes sense to do the qgis in a single batch and
#    set image refs as variables rather than static names. Single batch
#    processing would also allow us to spawn multiple instnaces of the python
#    script that does the qgis work.

require 'json'
require 'yaml'
require 'set'
require_relative '../ezqlib'
#require_relative '../ReportHandler/remove_blank_pages'

# Convert an Integer or Float to x.yy currency
def curr(v)
  return "%.2f" % v.to_f
end

# Gets all the files from S3 that we'll need based on refs in the input json.
# @param [Hash] input The input hash containing appropriate file refs.
# @return [Bool] true if all file gets succeeded; false otherwise
def get_files(input)
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


# Pulls map coords for each management zone from input and writes each
# set to a file named MZ_[id].geojson.
# @param [Hash] input The input hash containing management zones
# @return [nil] Side effects only; always returns nil.
def write_mz_coords(input)
  path = File.dirname(input['geojson_file'])
  input['scenarios'].each do |scenario|
    scenario.fetch('management_zones',[]).each do |mz|
      fname = "#{path}/MZ_#{mz['id']}.geojson"
      coords = mz['map_coords_as_geojson']
      File.write(fname,coords)
    end
  end
  return nil
end


# Run the binary that processes the input json and all the images.
# @return [Bool or nil] true, false, or nil as per EZQ.exec_cmd
def run_binary(input)
  return EZQ.exec_cmd("run_the_thing #{input.to_json.dump}")
end


def make_maps(input)
  in_dir = 'img_in'
  out_dir = 'report_images'

  # A bunch of calls to the agmap script will go here. This kind of thing:
  # Generate musym map
  #system("DISPLAY=:0 python agmap.py" +
  #       " --maptype=musym" +
  #       " --output=\"#{out_dir}/musym.png\"" +
  #       " --input=\"#{in_dir}/#{job_id}_#{record_id}.geojson\"" +
  #       " --autofit=exact")

end


################################################################################
# each of these needs to return the name of the file it generated
def make_yield_data(data)
  d = data.clone
  d[:yield_map] = ''# This will reference a file output by qgis
end

def make_applied_fertilizer(data)
  d = data.clone
end

def make_applied_planting(data)
  d = data.clone
end

def make_yield_by_soil(data)
  d = data.clone
end

def make_overall_profit(data)
  d = data.clone
  # !!!!! Some calculation?
  d[:field_revenue] = 0
  # !!!!! Some calculation?
  d[:field_expenses] = 0
  d[:field_profit] = d[:field_revenue] - d[:field_expenses]
  d[:field_profit_per_acre] = d[:field_profit]/d[:field_area]
  # !!!!! Some calculation?
  d[:field_roi] = 0
end

def make_overall_revenue_and_expenses(data,scenario)
  d = data.clone

  d[:commodity_price]
  d[:field_revenue]
  d[:field_expenses]
  d[:other_revenue]
  d[:total_revenue]
  d[:budget] = hash_budget(scenario['budget']['budget_items'],d[:field_area])
end

def make_zone_profit(data,zone)
  d = data.clone
  mz = zone.clone

  d[:mz_name] = mz['name']
  d[:mz_id] = mz['id']
  d[:mz_area] = mz['get_area_in_acres']

  budget_items = zone['budget']['budget_items']
  commodity_price = budget_items.select{|bi| bi['item_name'] == 'Commodity Price'}['amount']
  # !!!!! Somehow this will come in from Doug
  zone_yield = 0
  d[:mz_revenue] = commodity_price * zone_yield
    + budget_items.select{|bi| bi['item_name'] == 'Other Revenue'}['amount'] * d[:mz_area]

  revenues = ['Commodity Price','Other Revenue']
  d[:mz_expenses] = budget_items.select{|bi| !revenues.include?(bi['item_name'])}
                    .map{|i| i['amount']}.reduce(:+)

  d[:mz_profit] = d[:mz_revenue] - d[:mz_expenses]
  d[:mz_profit_per_acre] = d[:mz_profit]/d[:mz_area]
  # !!!!! Some calculation?
  d[:mz_roi] = 0
  d[:mz_year] = d[:scenario_budget]['name']
end

def make_revenue_and_expenses_with_zones(data,zone)
  d = data.clone
  mz = zone.clone

  d[:mz_name] = mz['name']
  d[:mz_area] = mz['get_area_in_acres']

  commodity_price = 0.00
  d[:mz_budget] = hash_budget(zone['budget']['budget_items'],d[:mz_area])
end
################################################################################

def hash_budget(budget_items,area)
  new_bud = {}
  return new_bud if !budget_items
  budget_items.each do |bi|
    bi['total'] = bi['amount'] * area if bi['unit_id'] == 1
    commodity_price = bi['amount'] if bi['unit_id'] == 2
    new_bud[bi['item_name']] = {:amount=>bi['total'],:itemized=>hash_budget(bi['sub_budget_items'],area)}
    new_bud[bi['item_name']][:units] = bi['unit_id'] == 1 ? '$/ac' : '$/bu'
  end
  return new_bud
end

def set_vars(input)
  j = input
  d = {}
  d[:field_name] = j['field_name']
  d[:field_area] = j['field_area']
  d[:scenario_name] = j['name']
  d[:scenario_budget] = j['budget']
  #d[:year] = j['budget']['year'] # no such key. The year appears be scenario_name

  # do whatever to flatten budget info into required form

  return d
end


def test
  AWS.config(YAML.load_file('../credentials.yml'))
  input = JSON.parse(File.read('field50pp.json'))
  #get_files(input)
  #write_mz_coords(input)
  #run_binary(input)
  #make_maps(input)

  reports = []
  scenario_ids = []
  input['scenarios'].each do |scenario|
    scenario_ids << scenario['id']

    # Add the field name and area into the top level of each scenario since
    # those values are needed for display in a few different places.
    scenario['field_name'] = input['name']
    scenario['field_area'] = input['get_area_in_acres']
    d = set_vars(scenario)

    pdfs = []
    # Make report sections for this scenario
    pdfs << make_yield_data(d)
    pdfs << make_applied_fertilizer(d)
    pdfs << make_applied_planting(d)
    pdfs << make_yield_by_soil(d)
    pdfs << make_overall_profit(d)
    pdfs << make_overall_revenue_and_expenses(d,scenario)

    scenario['management_zones'].each do |mz|
      pdfs << make_zone_profit(d,mz)
      pdfs << make_revenue_and_expenses_with_zones(d,mz)
    end


    report_name = ''  #  <~ Need to come up with naming scheme.

    # Stitch the report pieces into one pdf
    #system("gs -dBATCH -dNOPAUSE -q -sDEVICE=pdfwrite -dPDFSETTINGS=/prepress -sOutputFile=#{report_name} #{pdfs.join(' ')}")

    # Reject blank pages in the pdf. Set option :y to the same number as
    # .logo{max-height} from main.css for now.
    #RemoveBlankPages.remove(report_name,{:y=>100})
    reports << report_name
  end

  # Send reports to S3 and send back message with location, or email, or
  # whatever happens here
end

test
