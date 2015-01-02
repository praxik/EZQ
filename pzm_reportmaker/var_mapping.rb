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
  require 'fileutils'
  require '../ezqlib'
  require_relative './remove_blank_pages'
  require_relative './extractors'
  require_relative './side_effect_transforms'
  require_relative './page_makers'
end



# Convert an Integer or Float to x,xxx.yy (like currency)
def curr(v)
  whole,fraction = ("%.2f" % v.to_f).split('.')
  sign = whole[0] == '-' ? whole.slice!(0) : ''
  # Add commas for each group of 3 digits, counting right to left
  whole = whole.reverse.split(/([\d]{3})/).delete_if{|c| c==''}.join(',').reverse
  return sign + whole + '.' + fraction
end



def make_profit_maps(input)
  images = []
  make_legend = true

  # A bunch of calls to the agmap script will go here.
  Extractors.get_cids(input).each do |cid|
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

  input['scenarios'].each do |scenario|
    out_name = "report/#{scenario['id']}_yield.png"
    boundary_file = "report/field_3857_#{scenario['id']}.geojson"
    puts boundary_file
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
  histograms = []

  Extractors.get_cids(input).each do |cid|
    in_name = cid =~ /_/ ? "cb_images/#{cid}_zone_cb.tif.aux.xml" : "cb_images/#{cid}_cb.tif.aux.xml"
    out_name = "#{Dir.pwd()}/report/profit_hist_#{cid}.svg"
    cmd = "ruby hist.rb #{in_name} #{out_name}"
    histograms << out_name if EZQ.exec_cmd(cmd)
  end
  return histograms
end



def set_vars(input)
  j = input
  d = {}
  d[:field_name] = j['field_name']
  d[:field_area] = j['field_area']
  d[:scenario_name] = j['name']
  d[:scenario_id] = j['id']
  d[:scenario_budget] = j['budget']
  d[:zones] = j['management_zones']

  return d
end



def cleanup()
  @log.info "cleanup"
  #Delete contents of cb_images dir
  #FileUtils.rm_r('cb_images/*')

  #Delete contents of web_development dir
  #FileUtils.rm_r('web_development/*')

  #Delete contents of report dir *except* for the finished reports
  Dir.chdir('report')
  to_del = Dir.entries(Dir.pwd).reject{|f| f =~ /final_report\.pdf/}
  to_del = to_del - ['.','..','header.html','pzm-header-10.png']
  to_del.each{|f| File.unlink(f)}
  Dir.chdir('..')
end



def cp_if_not_exist(src,dst)
  FileUtils.cp(src,dst) if !File.exist?(dst)
end



def prep_report_dir
  Dir.mkdir('report') if !Dir.exist?('report')
  cp_if_not_exist('template/header.html','report/header.html')
  cp_if_not_exist('template/pzm-header-10.png','report/pzm-header-10.png')
end



def run
  # This is just for development in LightTable, since somehow my path gets munged:
  Dir.chdir('/home/penn/EZQ/pzm_reportmaker')

  get_reqs()

  @log = Logger.new(STDOUT)
  @log.level = Logger::INFO

  Dir.chdir('/home/penn/EZQ/pzm_reportmaker/test3')

  @log.info "Preparing report dir"
  prep_report_dir()

  AWS.config(YAML.load_file('credentials.yml'))
  input = JSON.parse(File.read('70.json'))

  Extractors.set_logger(@log)
#   Extractors.get_files(input)

  @log.info "Running binary"
  yield_data = Extractors.run_binary(input)

  @log.info "Reprojecting yield rasters into 3857"
  Extractors.get_yield_raster_hash(input).each{|r_in,r_out| SeTransforms.reproject_yield_raster(r_in,r_out)}

  @log.info "Reprojecting all boundaries into 3857"
  Extractors.get_boundary_hash(input).each{|k,v| SeTransforms.reproject_boundaries("report/#{k}_3857.geojson",v)}

  # Form up a geojson containing the field boundaries as well as all the
  # zone boundaries for each scenario
  @log.info "Collecting boundaries"
  input['scenarios'].each do |scenario|
    outname = "report/field_3857_#{scenario['id']}.geojson"
    if scenario.fetch('management_zones',[]).size > 0
        SeTransforms.collect_coords('report/field_3857.geojson',
          scenario['management_zones'].map{|z| "report/#{scenario['id']}_#{z['id']}_3857.geojson"},
          outname)
    else
      FileUtils.cp('report/field_3857.geojson', outname)
    end
  end

  @log.info "Making yield maps"
  make_yield_map(input)
  @log.info "Making profit maps"
  make_profit_maps(input)
  @log.info "Making profit histograms"
  make_profit_histograms(input)

  PageMakers.set_logger(@log)
  reports = []
  input['scenarios'].each do |scenario|

    # Add the field name and area into the top level of each scenario since
    # those values are needed for display in a few different places.
    scenario['field_name'] = input['name']
    scenario['field_area'] = input['get_area_in_acres']
    d = set_vars(scenario)
    d[:field_avg_yield] = yield_data[scenario['id']]

    pdfs = []
    pdfs << PageMakers.make_yield_data(d)
    #pdfs << PageMakers.make_applied_fertilizer(d)
    #pdfs << PageMakers.make_applied_planting(d)
    #pdfs << PageMakers.make_yield_by_soil(d)
    pdfs << PageMakers.make_overall_profit(d)
    pdfs << PageMakers.make_overall_revenue_and_expenses(d)

    scenario['management_zones'].each do |mz|
      pdfs << PageMakers.make_zone_profit(d,mz)
      pdfs << PageMakers.make_revenue_and_expenses_with_zones(d,mz)
    end

    report_name = "report/#{scenario['id']}_report.pdf"

    # Stitch the report pieces into one pdf
    @log.info "Combining pieces for #{report_name}"
    EZQ.exec_cmd("gs -dBATCH -dNOPAUSE -q -sDEVICE=pdfwrite -dPDFSETTINGS=/prepress -sOutputFile=#{report_name} #{pdfs.join(' ')}")

    # Reject blank pages in the pdf. Set option :y to the same number as
    # .logo{max-height} from main.css.
    @log.info "Removing blank pages"
    RemoveBlankPages.remove(report_name,{:y=>100})
    reports << report_name
  end

  # Stitch all the scenario reports into a single big report
  # Might need to add a job id into this naming scheme?
  report_name = "report/final_report.pdf"
  @log.info "Combining scenario reports into #{report_name}"
  EZQ.exec_cmd("gs -dBATCH -dNOPAUSE -q -sDEVICE=pdfwrite -dPDFSETTINGS=/prepress -sOutputFile=#{report_name} #{reports.join(' ')}")

  cleanup()
end

run
