#!/usr/bin/env ruby

require 'json'
require 'yaml'
require 'logger'
require 'fileutils'
require './ezqlib'
require_relative './remove_blank_pages'
require_relative './extractors'
require_relative './side_effect_transforms'
require_relative './page_makers'
require_relative './pdf_utils'
require_relative './dev_null_log'


# Monkeypatch NilClass so we don't have to test for nil before
# doing a fetch on the result of a select that may have returned nil
class NilClass
  def fetch(a,default)
    return default
  end
end

class PzmReportmaker

  def initialize(credentials,log=DevNullLog.new())
    @log = log

    Extractors.set_logger(@log)
    PageMakers.set_logger(@log)

    @log.info "Preparing report dir"
    prep_report_dir()

    AWS.config(YAML.load_file('credentials.yml'))
  end



  # Creates the report +output_path+ based on input from +input_path+
  # @param input_path [String] Input JSON file describing scenarios
  # @param output_path [String] Name to use for final pdf report
  #
  # Developer's note: This method should ideally contain minimal logic.
  # The goal is for it to read as a simple step-by-step sequence
  # of function calls for building a report.
  def make_report(input_path, output_path)

    input = JSON.parse(File.read(input_path));

    @log.info "Getting files from S3"
    Extractors.get_files(input)

    @log.info "Running binary"
    yield_data = Extractors.run_binary(input)
    if !yield_data
      @log.fatal "Binary returned no yield data; exiting."
      exit(1)
    end

    reproject_to_3857(input)

    collect_boundaries(input)

    make_maps(input)

    reports = input['scenarios'].map do |scenario|
      make_scenario_report(scenario,
                           input['name'],
                           input['get_area_in_acres'],
                           yield_data)
    end

    @log.info "Combining scenario reports into #{output_path}"
    AgPdfUtils.stitch(reports,"#{output_path}.numless")

    @log.info "Overlaying page numbers"
    PageMakers.make_number_overlay(AgPdfUtils.get_num_pages("#{output_path}.numless"))
    EZQ.exec_cmd("pdftk #{output_path}.numless multistamp report/page_numbers.pdf output #{output_path}.with_num")

    @log.info "Building table of contents"
    toc = PageMakers.make_toc(input,reports)

    @log.info "Adding table of contents to report"
    AgPdfUtils.stitch([toc, "#{output_path}.with_num"],output_path)

    @log.info "Removing intermediate files"
    cleanup(output_path)
  end


  ####################################################################################
  # Private interface from here on
  protected

  # Prepares report dir by creating it and copying over files that have to
  # be localized there.
  # @return nil
  def prep_report_dir
    Dir.mkdir('report') if !Dir.exist?('report')
    Dir.mkdir('cb_images') if !Dir.exist?('cb_images')
    cp_if_not_exist('template/header.html','report/header.html')
    cp_if_not_exist('template/pzm-header-10.png','report/pzm-header-10.png')
    cp_if_not_exist('multido.sty','report/multido.sty')
    cp_if_not_exist('multido.tex','report/multido.tex')
    return nil
  end



  # Returns a hash containing the path to every profit raster reference in
  # input as the key, and the expected name of the 3857 re-projection of same
  # as the value.
  # @param [Hash] input Ruby Hash of the full JSON input
  # @return [Hash]
  def get_profit_raster_hash(input)
    return Extractors.get_cids(input).map do |cid|
      tiff = cid =~ /_/ ? "cb_images/#{cid}_zone_cb.tif" : "cb_images/#{cid}_cb.tif"
      [tiff,"#{tiff}.3857"]
    end.to_h
  end



  # Makes all profit maps required by the report
  # @param [Hash] input Ruby Hash of the full JSON input
  # @return [Array] List of all output image paths
  def make_profit_maps(input)
    images = []
    make_legend = true

    Extractors.get_cids(input).each do |cid|
      out_name = "report/profit_map_#{cid}.png"
      tiff = cid =~ /_/ ? "cb_images/#{cid}_zone_cb.tif.3857" : "cb_images/#{cid}_cb.tif.3857"
      boundary_file = "report/field_3857_#{cid.split('_').first}.geojson"

      # Need to add DISPLAY=:0 back into this on AWS?
        cmd = "python agmap.py" +
              " --maptype=aerial" +
              " --output=\"#{out_name}\"" +
              " --input=\"#{boundary_file}\"" +
              " --inputbudget=\"#{tiff}\"" +
              " --qmlfile=\"template/QMLFiles/bnd_blue_nameoutline.qml\"" +
              " --qmlfileBudget=\"template/QMLFiles/Profitn500t500w100.qml\"" +
              " --width=630 --height=630 --autofit=false"

        # Set margin for the zone profit maps
        cmd += cid =~ /_/ ? " --margin=100" : ""

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
        @log.error res.last
      end
    end

    return images
  end



  # Makes all yield maps (and yield legends) required by the report
  # @param [Hash] input Ruby Hash of the full JSON input
  # @return [Array] List of all output image paths
  def make_yield_map(input)
    images = []

    input['scenarios'].each do |scenario|
      out_name = "report/#{scenario['id']}_yield.png"
      boundary_file = "report/field_3857_#{scenario['id']}.geojson"
      @log.debug boundary_file
      cmd =  "python agmap.py" +
             " --maptype=aerial" +
             " --output=\"#{out_name}\"" +
             " --input=\"#{boundary_file}\"" +
             " --inputyield=\"report/#{scenario['id']}_yield.tiff\"" +
             " --qmlfile=\"template/QMLFiles/bnd_blue_nameoutline.qml\"" +
             " --width=630 --height=630 --autofit=false" +
             #" --margin=-9999" +
             " --legendtype=yield" +
             " --legendformat=png" +
             " --legendfile=report/#{scenario['id']}_yield_legend.png"
      @log.info "Yield map: #{out_name}"
      @log.debug cmd
      res = EZQ.exec_cmd(cmd)
      images << out_name if res.first
      @log.error res.last if !res.first
    end

    return images
  end



  # Makes all profit histograms required by the report
  # @param [Hash] input Ruby Hash of the full JSON input
  # @return [Array] List of all output image paths
  def make_profit_histograms(input)
    histograms = []

    Extractors.get_cids(input).each do |cid|
      in_name = cid =~ /_/ ? "cb_images/#{cid}_zone_cb.tif.3857.aux.xml" : "cb_images/#{cid}_cb.tif.3857.aux.xml"
      out_name = "#{Dir.pwd()}/report/profit_hist_#{cid}.svg"
      cmd = "ruby hist.rb #{in_name} #{out_name}"
      @log.debug cmd
      histograms << out_name if EZQ.exec_cmd(cmd)
    end
    return histograms
  end


  # Recipe that calls into each of the image-making functions
  # in turn.
  # @param [Hash] input Ruby Hash of the full JSON input
  # @return nil
  def make_maps(input)
    @log.info "Making yield maps"
    make_yield_map(input)
    @log.info "Making profit maps"
    make_profit_maps(input)
    @log.info "Making profit histograms"
    make_profit_histograms(input)
    return nil
  end


  # Cleans up intermediate files downloaded or created in the course
  # of building a report
  # @return nil
  def cleanup(output_path)
    @log.info "cleanup"

    FileUtils.rm_r('cb_images') if Dir.exist?('cb_images')
    FileUtils.rm_r('web_development') if Dir.exist?('web_development')

    # Delete contents of report dir *except* for the finished report
    Dir.chdir('report')
    # Assumes there's no extra path depth on output_path....
    to_del = Dir.entries(Dir.pwd).reject{|f| f == File.basename(output_path)}
    to_del = to_del - ['.','..','header.html','pzm-header-10.png','multido.sty','multido.tex']
    to_del.each{|f| File.unlink(f)}
    Dir.chdir('..')

    # Bing stuff is written to same dir the agmap.py script lives in.
    bing_images = Dir.entries(Dir.pwd).select{|f| f =~ /(^L|^midL).+(\.png|\.tiff|\.xml)/}
    bing_images.each{|f| File.unlink(f)}

    return nil
  end



  # Copies file src to dest iff dst does not exist
  # @return nil
  def cp_if_not_exist(src,dst)
    FileUtils.cp(src,dst) if !File.exist?(dst)
    return nil
  end



  # Reprojects all (necessary) rasters and geojson referenced in the input
  # @param [Hash] Ruby Hash of full JSON input
  # @return nil
  def reproject_to_3857(input)
    @log.info "Reprojecting yield rasters into 3857"
    Extractors.get_yield_raster_hash(input).each{|r_in,r_out| SeTransforms.reproject_raster(r_in,r_out)}
    @log.info "Reprojecting profit rasters into 3857"
    get_profit_raster_hash(input).each{|r_in,r_out| SeTransforms.reproject_raster(r_in,r_out)}

    @log.info "Reprojecting all boundaries into 3857"
    Extractors.get_boundary_hash(input).each{|k,v| SeTransforms.reproject_boundaries("report/#{k}_3857.geojson",v)}
    return nil
  end



  # Forms up a geojson containing the field boundaries as well as all the
  # zone boundaries for each scenario
  # @param input [Hash] Ruby Hash of full JSON input
  # @return nil
  def collect_boundaries(input)
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
    return nil
  end



  # Makes the report (piece) for a single scenario
  # @parm [Hash] scenario Ruby Hash of a single scenario
  # @param [String] name Field name
  # @param [Float] area Field area in acres
  # @param [Hash] yield_data Hash of yield data as returned by the binary
  #               6k_pzm_report
  # @return [String] Name of created report
  def make_scenario_report(scenario,name,area,yield_data)
    scenario['field_name'] = name
    scenario['field_area'] = area
    d = {}
    d[:field_name] = scenario['field_name']
    d[:field_area] = scenario['field_area']
    #d[:field_yield] = scenario['field_yield_value'] # We don't use this anymore.
    d[:scenario_name] = scenario['name']
    d[:scenario_id] = scenario['id']
    d[:scenario_budget] = scenario['budget']
    d[:zones] = scenario['management_zones']
    d[:year] =  scenario.fetch('year',2040)
    d[:field_avg_yield] = yield_data[scenario['id']][:avg]
    d[:nz_yield] = yield_data[scenario['id']][:nz]

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

    @log.info "Combining pieces for #{report_name}"
    AgPdfUtils.stitch(pdfs,report_name)

    # Set option :y to the same number as .logo{max-height} from main.css.
    @log.info "Removing blank pages"
    RemoveBlankPages.remove(report_name,{:y=>100})

    return report_name
  end


end #class
