#!/usr/bin/env ruby


# Monkeypatch NilClass so we don't have to test for nil before
# doing a fetch on the result of a select that may have returned nil
class NilClass
  def fetch(a,default)
    return default
  end
end



def get_reqs
  require 'json'
  require 'yaml'
  require 'set'
  require 'pdfkit'
  require 'erb'
  require 'logger'
  require '../ezqlib'
  require_relative './remove_blank_pages'
end



# Convert an Integer or Float to x,xxx.yy (like currency)
def curr(v)
  whole,fraction = ("%.2f" % v.to_f).split('.')
  sign = whole[0] == '-' ? whole.slice!(0) : ''
  # Add commas for each group of 3 digits, counting right to left
  whole = whole.reverse.split(/([\d]{3})/).delete_if{|c| c==''}.join(',').reverse
  return sign + whole + '.' + fraction
end



# Gets all the files from S3 that we'll need based on refs in the input json.
# @param [Hash] input The input hash containing appropriate file refs.
# @return [Bool] true if all file gets succeeded; false otherwise
def get_files(input)
  @log.info "get_files"
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
def run_binary(input)
  @log.info "run_binary"
  f = "in.json"
  File.unlink(f) if File.exists?(f)
  File.write(f,input.to_json)
  yields = nil
  success,output = EZQ.exec_cmd("LD_LIBRARY_PATH=. SAGA_MLB=./saga ./6k_pzm_report -p #{f} -o cb_images/")
  @log.error output if !success
  if success and output.size > 0
    # Convert array of strings that looks like ["junk\n",
    #                                           "pzm: 132: 234.3\n",
    #                                           "more junk\n",
    #                                           "stuff\n",
    #                                           "pzm: 144: 45.323\n"]
    # into a hash that looks like: {132 => 234.3, 144 => 45.323}
    yields = output.select{|t| t =~ /^pzm/}
    yields = yields.map{|t| t.chomp.gsub('pzm: ','').split(': ')}
    yields = yields.map{|a| [a[0].to_i,a[1].to_f]}.to_h
  end
  # Return either a hash or nil
  return yields
end



# Returns a hash in which each key is an input yield raster and the
# associated value is the desired name of the reprojected raster.
# @param [Hash] input Ruby hash containing the full JSON sent by web app
def get_yield_raster_hash(input)
  hash = {}
  input['scenarios'].each do |scenario|
    raster_in = scenario['get_tiff_raster_path']
    raster_out = "report/#{scenario['id']}_yield.tiff"
    hash[raster_in] = raster_out
  end
  return hash
end



# Reprojects raster_in to EPSG:3857 and saves as raster_out
# @param [String] raster_in Path to input raster
# @param [String] raster_out Path to output raster
def reproject_yield_raster(raster_in,raster_out)
  @log.info "reproject_yield_raster #{raster_in} to #{raster_out}"
  cmd = "gdalwarp -r cubicspline -t_srs EPSG:3857 \"#{raster_in}\" \"#{raster_out}\""
  @log.debug cmd
  EZQ.exec_cmd(cmd)
end



# Returns a hash containing a cid as the key and map coords as the value.
# One entry in the hash has the key "field", which refers to the
# whole-field coordinates.
def get_boundary_hash(input)
  coords = {}
  coords['field'] = input['map_coords_as_geojson']
  input['scenarios'].each do |scenario|
    scenario['management_zones'].each do |mz|
      coords["#{scenario['id']}_#{mz['id']}"] = mz['map_coords_as_geojson']
    end
  end
  return coords
end



# Reprojects a hash of geojson boundaries to EPSG:3857 and stores in a file.
# Side-effects only; no useful return value.
# @param [String] out_name Path to file that should hold the reprojected bounds
# @param [String] coords GeoJSON string of coordinates to reproject
# @return nil
def reproject_boundaries(out_name,coords)
  @log.info "reproject_boundaries: #{out_name}"
  in_name = "#{out_name}.tmp.geojson"
  File.write(in_name,coords)
  cmd = "ogr2ogr -t_srs EPSG:3857 -f \"GeoJSON\" #{out_name} #{in_name}"
  EZQ.exec_cmd(cmd)
  return nil
end



def collect_coords(master,pieces_array,collected_name)
  # Convert field boundary to shapefile
  EZQ.exec_cmd("ogr2ogr -f \"ESRI Shapefile\" #{master}.shp #{master}")

  # Append each zone boundary to the field shapefile.
  pieces_array.each do |f|
    EZQ.exec_cmd("ogr2ogr -update -append -f \"ESRI Shapefile\" #{master}.shp #{f}")
  end

  # Convert the shapefile back to geojson.
  EZQ.exec_cmd("ogr2ogr -f \"GeoJSON\" #{collected_name} #{master}.shp")

  return nil
end



def get_scenario_tiff_hash(input)
  hash = {}
  input['scenarios'].each do |scenario|
    scenario_id = scenario['id']
    tiff = scenario['get_tiff_raster_path']
    hash[scenario_id] = tiff
  end
  return hash
end



def get_cids(input)
  cids = []
  input['scenarios'].each do |scenario|
    cids << "#{scenario['id']}"
    scenario['management_zones'].each do |mz|
      cids << "#{scenario['id']}_#{mz['id']}"
    end
  end
  return cids
end



def make_profit_maps(input)
  @log.info "make_maps"
  images = []
  make_legend = true

  # A bunch of calls to the agmap script will go here.
  get_cids(input).each do |cid|
    out_name = "report/profit_map_#{cid}.png"
    tiff = cid =~ /_/ ? "cb_images/#{cid}_zone_cb.tif" : "cb_images/#{cid}_cb.tif"
    # Need to add DISPLAY=:0 back into this on AWS?
    cmd = "python agmap.py" +
          " --maptype=budget" +
          " --output=\"#{out_name}\"" +
          " --input=\"#{tiff}\"" +
          " --qmlfile=\"template/QMLFiles/Profitn500t500w100.qml\"" +
          " --width=2000 --height=2000 --autofit=true" +
          " --overlayinput=report/field_3857_2.geojson --overlayfillcolor=255,0,0,255"

    if make_legend
      cmd = cmd +
            " --legendtype=profit" +
            " --legendformat=png" +
            " --legendfile=report/profit_legend.png"
      make_legend = false
    end

    @log.debug "\t#{out_name}"
    @log.debug cmd
    res = EZQ.exec_cmd(cmd)
    if res.first
      images << out_name
    else
      @log.error res
    end
  end

  return images
end



def make_yield_map(input)
  images = []
  boundary_file = 'report/field_3857_2.geojson'

  input['scenarios'].each do |scenario|
    out_name = "report/#{scenario['id']}_yield.png"
    cmd =  "python agmap.py" +
           " --maptype=aerial" +
           " --output=\"#{out_name}\"" +
           " --input=\"#{boundary_file}\"" +
           " --inputdem=\"report/#{scenario['id']}_yield.tiff\"" +
           " --qmlfile=\"template/QMLFiles/bnd_blue_nameoutline.qml\"" +
           " --width=2000 --height=2000 --autofit=false"
    @log.info "Yield map: #{out_name}"
    res = EZQ.exec_cmd(cmd)
    images << out_name if res.first
    @log.error res[1] if !res.first
  end

  return images
end



def make_profit_histograms(input)
  @log.info "make_profit_histograms"
  histograms = []

  get_cids(input).each do |cid|
    in_name = "cb_images/#{cid}_zone_cb.tif.aux.xml"
    out_name = "#{Dir.pwd()}/report/profit_hist_#{cid}.svg"
    cmd = "ruby hist.rb #{in_name} #{out_name}"
    histograms << out_name if EZQ.exec_cmd(cmd)
  end
  return histograms
end



def make_expenses_pie_chart(budget_items,out_file)
  @log.debug "make_expenses_pie_chart"
  sb = budget_items
  values = sb.map{|bi| bi['amount']}
  labels = sb.map{|bi| bi['item_name']}
  labels = labels.map{|t| t.gsub(/\//,"/\n")}
  return EZQ.exec_cmd(['python', 'pie_chart.py',out_file,labels.to_s,values.to_s])
end


################################################################################
# each of these needs to return the name of the file it generated

def make_yield_data(data)
  d = data.clone
  d[:yield_map] = "#{d[:scenario_id]}_yield.png"
  return make_pdf(generate_html(d,'template/yield_data.html.erb',
    "report/#{d[:scenario_id]}_yield_data.html"))
end



def make_applied_fertilizer(data)
  d = data.clone
  return make_pdf(generate_html(d,'template/applied_fertilizer.html.erb',
                       "report/#{d[:scenario_id]}_applied_fertilizer.html"))
end



def make_applied_planting(data)
  d = data.clone
  return make_pdf(generate_html(d,'template/applied_planting.html.erb',
                       "report/#{d[:scenario_id]}_applied_planting.html"))
end



def make_yield_by_soil(data)
  d = data.clone
  return make_pdf(generate_html(d,'template/yield_by_soil.html.erb',
                       "report/#{d[:scenario_id]}_yield_by_soil.html"))
end



def make_overall_profit(data)
  d = data.clone

  d[:field_revenue] = calculate_total_revenue(d[:scenario_budget],d[:field_avg_yield],d[:field_area])
  d[:field_expenses_per_acre] = calculate_expenses_per_acre(d[:scenario_budget])
  d[:field_expenses] = d[:field_expenses_per_acre] * d[:field_area]
  d[:field_profit] = d[:field_revenue] - d[:field_expenses]
  d[:field_profit_per_acre] = d[:field_profit] / d[:field_area]

  d[:field_roi] = d[:field_profit] / d[:field_expenses]

  # Image paths
  cid = "#{d[:scenario_id]}"
  d[:field_profit_map] = "profit_map_#{cid}.png"
  d[:field_histogram] = "profit_hist_#{cid}.svg"
  d[:field_pie_chart] = "expenses_pie_#{cid}.svg"

  set_overall_expense_revenue_vars(d)

  make_expenses_pie_chart(d[:budget_exp],"report/#{d[:field_pie_chart]}")

  return make_pdf(generate_html(d,'template/overall_profit.html.erb',
                       "report/#{d[:scenario_id]}_overall_profit.html"))
end



def make_zone_profit(data,zone)
  d = data.clone
  mz = zone.clone

  do_zone_calcs!(d,mz)
#   d[:mz_revenue] = calculate_total_revenue(zone['budget'],
#                                      d[:mz_avg_yield]*d[:mz_area],
#                                      d[:mz_area])
  # Image paths
  cid = "#{d[:scenario_id]}_#{d[:mz_id]}"
  d[:mz_profit_map] = "profit_map_#{cid}.png"
  d[:mz_histogram] = "profit_hist_#{cid}.svg"
  d[:mz_pie_chart] = "expenses_pie_#{cid}.svg"

  make_expenses_pie_chart(d[:mz_budget_exp],"report/#{d[:mz_pie_chart]}")

  return make_pdf(generate_html(d,'template/zone_profit.html.erb',
                       "report/#{d[:scenario_id]}_zone_#{d[:mz_id]}_profit.html"))
end



def make_overall_revenue_and_expenses(data)
  d = data.clone

  set_overall_expense_revenue_vars(d)

  return make_pdf(generate_html(d,'template/overall_revenue_and_expenses.html.erb',
                                  "report/#{d[:scenario_id]}_overall_revenue_and_expenses.html"))
end



def make_revenue_and_expenses_with_zones(data,zone)
  d = data.clone
  mz = zone.clone

  do_zone_calcs!(d,mz)

  if false
    puts "Zone yield (bu/ac) #{d[:mz_avg_yield]}"
    puts "Zone area (ac) #{d[:mz_area]}"
    puts "Zone profit ($/ac) #{d[:mz_profit_per_acre]}"
    puts "Zone expense ($) #{d[:mz_expenses_per_acre] * d[:mz_area]}"
    puts "Zone revenue ($) #{d[:mz_revenue_per_acre] * d[:mz_area]}"
    puts "Zone profit ($) #{d[:mz_profit_per_acre] * d[:mz_area]}"
    puts "Zone ROI (%) #{d[:mz_profit_per_acre] / d[:mz_expenses_per_acre]}"
    puts ''
  end

  return make_pdf(generate_html(d,'template/revenue_and_expenses_with_zones.html.erb',
                       "report/#{d[:scenario_id]}_zone_#{d[:mz_id]}_revenue_and_expenses.html"))
end
################################################################################



def do_zone_calcs!(d,mz)
  d[:mz_name] = mz['name']
  d[:mz_id] = mz['id']
  d[:mz_area] = mz['get_area_in_acres']
  d[:mz_commodity_price] = mz['budget']['budget_items'].select{|bi| bi['item_name'] == 'Commodity Price'}.first.fetch('amount',0)

  set_overall_expense_revenue_vars(d)

  d[:mz_avg_yield] = mz['get_target_zone_yield']
  d[:mz_revenue] = d[:mz_commodity_price] * d[:mz_avg_yield] * d[:mz_area]
  d[:mz_expenses_per_acre] = calculate_expenses_per_acre(mz['budget'])
  d[:mz_expenses] = d[:mz_expenses_per_acre] * d[:mz_area]
  d[:mz_other_revenue_per_acre] = mz['budget']['budget_items'].select{|bi| bi['item_name'] == 'Other Revenue'}.first.fetch('amount',0)
  d[:mz_revenue_per_acre] = d[:mz_revenue] / d[:mz_area] +
                              d[:mz_other_revenue_per_acre]
  d[:mz_profit_per_acre] = d[:mz_revenue_per_acre] - d[:mz_expenses_per_acre]
  d[:mz_profit] = d[:mz_profit_per_acre] * d[:mz_area]
  d[:mz_roi] = d[:mz_profit_per_acre] / d[:mz_expenses_per_acre] * 100
  d[:mz_year] = d[:scenario_budget]['name']

  d[:mz_budget_exp] = sort_budget(mz['budget']['budget_items'],'item_id')
                                   .delete_if{|it| !it['expense']}
  return nil
end



def calculate_total_revenue(budget,yld,area)
  items = budget['budget_items']
  commodity_price = items.select{|bi| bi['item_name'] == 'Commodity Price'}[0]['amount']
  return commodity_price * yld * area +
    items.select{|bi| bi['item_name'] == 'Other Revenue'}.first.fetch('amount',0) * area
end



def calculate_expenses_per_acre(budget)
  expenses = budget['budget_items'].select{|bi| bi['expense'] == true}
                    .map{|i| i['amount']}.reduce(:+)
  expenses = 1.0 if !expenses or expenses == 0.0
  return expenses
end


def set_overall_expense_revenue_vars(d)
  d[:commodity_price] = d[:scenario_budget]['budget_items'].select{|bi| bi['item_name'] == 'Commodity Price'}[0]['amount']
  d[:field_revenue_per_acre] = d[:commodity_price] * d[:field_avg_yield]
  d[:total_expenses_per_acre] = calculate_expenses_per_acre(d[:scenario_budget])
  d[:other_revenue_per_acre] = d[:scenario_budget]['budget_items'].select{|bi| bi['item_name'] == 'Other Revenue'}.first.fetch('amount',0)
  d[:total_revenue_per_acre] = d[:field_revenue_per_acre] +
                                d[:other_revenue_per_acre]
  d[:field_expenses] = d[:total_expenses_per_acre] * d[:field_area]
  d[:total_profit_per_acre] = d[:total_revenue_per_acre] -
                               d[:total_expenses_per_acre]
  # We want a sorted budget that includes only expenses
  d[:budget_exp] = sort_budget(d[:scenario_budget]['budget_items'],'item_id')
                    .select{|it| it['expense'] == true}
  return nil
end



# Run erb to expand template in_file and write the html result to out_file
# @return Returns the value passed in as parameter out_file
def generate_html(d,in_file,out_file)
  @log.info "Generating #{out_file}"
  out_file = File.absolute_path(out_file)
  pwd = Dir.pwd()
  # Changing into in_file's directory ensures that all relative
  # paths mentioned in in_file work properly
  Dir.chdir(File.dirname(in_file))
  erbed = ERB.new(File.read(File.basename(in_file)))

  File.write(out_file,erbed.result(binding))
  Dir.chdir(pwd)

  return out_file
end



def make_pdf(html,header='header.html')
  #return html
  @log.info "make_pdf"
  header_in = header #File.absolute_path(header)
  pwd = Dir.pwd()
  Dir.chdir(File.dirname(html))
  html_in = File.basename(html)

  pdfkit = PDFKit.new(File.new("#{html_in}"),
                    :page_size => 'Letter',
                    :margin_left => '2mm',
                    :margin_right => '2mm',
                    :margin_top => '35mm',
                    :margin_bottom => '10mm',
                    :header_html => header_in
                    )

  pdf_file = "#{html_in}.pdf"
  pdfkit.to_file(pdf_file)
  # Undo the chdir from above
  Dir.chdir(pwd)
  # return filename of generated pdf
  return "#{html}.pdf"
end



# Sort the budget items by item_id so they can be displayed in the correct order
# with minimal fuss in the erb. Will also sort sub_items and strip them out
# if sum(subs) != parent amount
def sort_budget(budget_items,key)
  sorted = budget_items.sort_by{|it| it[key]}
  sorted.each do |bi|
    if bi.fetch('sub_budget_items',[]).size > 0
      # Can't rely on strict equality because we're dealing with rounded floating point numbers.
      if (bi['sub_budget_items'].map{|sbi| sbi['amount']}.reduce(:+) - bi['amount']).abs() < 0.001
        bi['sub_budget_items'] = sort_budget(bi['sub_budget_items'],key)
      else
        bi['sub_budget_items'] = []
      end
    end
  end
  return sorted
end



def set_vars(input)
  j = input
  d = {}
  d[:field_name] = j['field_name']
  d[:field_area] = j['field_area']
  d[:scenario_name] = j['name']
  d[:scenario_id] = j['id']
  d[:scenario_budget] = j['budget']
  #d[:year] = j['budget']['year'] # no such key. The year appears be scenario_name

  return d
end



def cleanup()
  #Delete contents of cb_images dir
  #FileUtils.rm_r('cb_images/*')

  #Delete contents of web_development dir
  #FileUtils.rm_r('web_development/*')

  #Delete contents of report dir *except* for the finished reports
  Dir.chdir('report')
  to_del = Dir.entries.reject{|f| f =~ /report\.pdf/}
  to_del = to_del - ['.','..','header.html','isa-header-narrow.png']
  #to_del.each{|f| File.unlink(f)}
  puts to_del
  Dir.chdir('..')
end



def test
  # This is just for development in LightTable, since somehow my path gets munged:
  Dir.chdir('/home/penn/EZQ/pzm_reportmaker')

  get_reqs()

  @log = Logger.new(STDOUT)
  @log.level = Logger::INFO

  Dir.chdir('/home/penn/EZQ/pzm_reportmaker/test2')

  AWS.config(YAML.load_file('credentials.yml'))
  input = JSON.parse(File.read('70.json'))
  #get_files(input)

  yield_data = run_binary(input)

  # Reproject the yield rasters into 3857
  get_yield_raster_hash(input).each{|r_in,r_out| reproject_yield_raster(r_in,r_out)}

  # Reproject all boundaries into 3857
  get_boundary_hash(input).each{|k,v| reproject_boundaries("report/#{k}_3857.geojson",v)}

  # Form up a geojson containing the field boundaries as well as all the
  # zone boundaries
  collect_coords('report/field_3857.geojson',
    get_boundary_hash(input).reject{|k,v| k == 'field'}.map{|k,v| "report/#{k}_3857.geojson"},
    'report/field_3857_2.geojson')

  make_yield_map(input)
  #make_profit_maps(input)
  #make_profit_histograms(input)

  reports = []
  scenario_ids = []
  input['scenarios'].each do |scenario|
    scenario_ids << scenario['id']

    # Add the field name and area into the top level of each scenario since
    # those values are needed for display in a few different places.
    scenario['field_name'] = input['name']
    scenario['field_area'] = input['get_area_in_acres']
    d = set_vars(scenario)
    d[:field_avg_yield] = yield_data[scenario['id']]

    pdfs = []
    pdfs << make_yield_data(d)
    #pdfs << make_applied_fertilizer(d)
    #pdfs << make_applied_planting(d)
    #pdfs << make_yield_by_soil(d)
    pdfs << make_overall_profit(d)
    pdfs << make_overall_revenue_and_expenses(d)

    scenario['management_zones'].each do |mz|
      pdfs << make_zone_profit(d,mz)
      pdfs << make_revenue_and_expenses_with_zones(d,mz)
    end

    # Might need to add a job id into this naming scheme?
    report_name = "report/#{scenario['id']}_report.pdf"

    # Stitch the report pieces into one pdf
    system("gs -dBATCH -dNOPAUSE -q -sDEVICE=pdfwrite -dPDFSETTINGS=/prepress -sOutputFile=#{report_name} #{pdfs.join(' ')}")

    # Reject blank pages in the pdf. Set option :y to the same number as
    # .logo{max-height} from main.css for now.
    @log.info "Remove blank pages"
    RemoveBlankPages.remove(report_name,{:y=>100})
    reports << report_name
  end

  # Stitch all the scenario reports into a single big report
  report_name = "report/final_report"
  system("gs -dBATCH -dNOPAUSE -q -sDEVICE=pdfwrite -dPDFSETTINGS=/prepress -sOutputFile=#{report_name} #{reports.join(' ')}")

  #cleanup()
end

test
