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
  #require_relative '../ReportHandler/remove_blank_pages'
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
  @log.info "run_binary"
  return EZQ.exec_cmd("run_the_thing #{input.to_json.dump}")
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



def make_maps(input)
  @log.info "make_maps"
  in_dir = 'img_in'
  out_dir = 'report_images'
  images = []
  do_once = true

  # A bunch of calls to the agmap script will go here.
  get_cids(input).each do |cid|
    out_name = "report/profit_map_#{cid}.png"
    tiff = "cb_images/#{cid}_zone_cb.tif"
    # Need to add DISPLAY=:0 back into this on AWS?
    cmd = "python agmap.py" +
          " --maptype=budget" +
          " --output=\"#{out_name}\"" +
          " --input=\"#{tiff}\"" +
          " --qmlfile=\"template/QMLFiles/Profitn500t500w100.qml\"" +
          " --width=2000 --height=2000 --autofit=true"
    if do_once
      cmd = cmd +
            " --legendtype=profit" +
            " --legendformat=png" +
            " --legendfile=report/profit_legend.png"
      do_once = false
    end

    images << out_name if EZQ.exec_cmd(cmd)
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
  # Use dem mode of agmap and first raw raster refd in json to gen the image
  # needed here.
  d = data.clone
  #d[:yield_map] = ''# This will reference a file output by qgis
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

  # !!!!! Somehow this will come in from Doug
  field_yield = 1
  d[:field_revenue] = calculate_total_revenue(d[:scenario_budget],field_yield,d[:field_area])
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
  return commodity_price * yld +
    items.select{|bi| bi['item_name'] == 'Other Revenue'}.first.fetch('amount',0) * area
end



def calculate_expenses_per_acre(budget)
  expenses = budget['budget_items'].select{|bi| bi['expense'] == true}
                    .map{|i| i['amount']}.reduce(:+)
  expenses = 1.0 if !expenses or expenses == 0.0
  return expenses
end


def set_overall_expense_revenue_vars(d)
  d[:commodity_price] = d[:scenario_budget]['budget_items']
              .select{|bi| bi['item_name'] == 'Commodity Price'}[0]['amount']
  # !!!!! Somehow this will come in from Doug
  d[:field_avg_yield] = 1
  d[:field_revenue] = d[:commodity_price] * d[:field_avg_yield]
  d[:total_expenses_per_acre] = calculate_expenses_per_acre(d[:scenario_budget])
  d[:other_revenue_per_acre] = d[:scenario_budget]['budget_items'].select{|bi| bi['item_name'] == 'Other Revenue'}.first.fetch('amount',0)
  d[:total_revenue_per_acre] = d[:field_revenue] +
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
  return html
  pwd = Dir.pwd()
  Dir.chdir(File.dirname(html))
  html_in = File.basename(html)

  header_in = File.basename(header)

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
  return pdf_file
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

  # do whatever to flatten budget info into required form

  return d
end



def run
  get_reqs()
  AWS.config(YAML.load_file('../credentials.yml'))
  input = JSON.parse(File.read('70.json'))
  get_files(input)
  yield_data = JSON.parse(run_binary(input))
  make_maps(input)
  make_profit_histograms(input)

  reports = []
  scenario_ids = []
  input['scenarios'].each do |scenario|
    scenario_ids << scenario['id']

    # Add the field name and area into the top level of each scenario since
    # those values are needed for display in a few different places.
    scenario['field_name'] = input['name']
    scenario['field_area'] = input['get_area_in_acres']
    d = set_vars(scenario)
    d[:yield_data] = yield_data

    pdfs = []
    # Make report sections for this scenario
    pdfs << make_yield_data(d)
    pdfs << make_applied_fertilizer(d)
    pdfs << make_applied_planting(d)
    #pdfs << make_yield_by_soil(d)
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




def test
  # This is just for development in LightTable, since somehow my path gets munged:
  Dir.chdir('/home/penn/EZQ/pzm_reportmaker')

  get_reqs()

  @log = Logger.new(STDOUT)
  @log.level = Logger::INFO

  Dir.chdir('/home/penn/EZQ/pzm_reportmaker/test')

  AWS.config(YAML.load_file('credentials.yml'))
  input = JSON.parse(File.read('72.json'))
  #make_maps(input)
  #make_profit_histograms(input)

  #get_files(input)
  #write_mz_coords(input)

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
    pdfs << make_overall_profit(d)
    pdfs << make_overall_revenue_and_expenses(d)

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


  #puts input['scenarios'][0]['budget']['budget_items']
  #puts make_expenses_pie_chart(input['scenarios'][1]['budget']['budget_items'],'pie_test.png')
end

test
