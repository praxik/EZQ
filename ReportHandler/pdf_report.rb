require 'erb'
require 'pdfkit'
require 'aws-sdk'
require 'yaml'
require 'nokogiri'
require 'fileutils'
require './remove_blank_pages'

# Module containing function(s) for making pdf reports
module PdfReport

# Makes a pdf report from a pair of templates and a hash of data
# @param template [String] path to the main erb template for the report
# @param header [String] path to the file to be used as the header on *every*
#                        page of the report. This file can be html or erb, but
#                        must evaluate to a full, valid html document in either
#                        case.
# @param data [Hash] hash containing any data required by the erb template. It's
#                    up to the caller to ensure the format of the data structure
#                    matches that of the erb template. The erb should expect to
#                    get the "data" from a variable named "data".
# @param last_name [String] the last piece of the filename just before .pdf
#                           PDFs will be saved with the name
#                           [job_id]_[record_id]_[last_name].pdf
# @return returns the name of the generated file
def self.make_pdf(template,header,data,last_name)
  Dir.chdir(File.dirname(template))
  html_in = File.basename(template)
  input = File.read(html_in)

  header = File.read(header)

  erbed = ERB.new(input)

  File.write("#{last_name}.html",erbed.result)

  pdfkit = PDFKit.new(File.new("#{last_name}.html"),
                    #:print_media_type => true,
                    :page_size => 'Letter',
                    :margin_left => '2mm',
                    :margin_right => '2mm',
                    :margin_top => '35mm',
                    :margin_bottom => '10mm',
                    #:footer_center => 'Page [page] of [topage]',
                    :header_html => 'header.html'
                    )

  pdf_file = "report_data/#{data[:job_id]}_#{data[:record_id]}_#{last_name}.pdf"
  pdfkit.to_file("../#{pdf_file}")
  # Undo the chdir from above
  Dir.chdir('..')
  # return filename of generated pdf
  return pdf_file
end



def self.make_gis_images(data,field_data)

  soil_mapper = "Soil.py"
  budget_mapper = "Budget.py"

  in_dir = "report_data"
  out_dir = "report_data"
  json_dir = "json"

  job_id = data[:job_id]
  record_id = data[:record_id]

  soil_file = "#{in_dir}/#{job_id}_#{record_id}.geojson"

  # Generate musym map
  system("DISPLAY=:0 python agmap.py" +
         " --maptype=musym" +
         " --output=\"#{out_dir}/musym.png\"" +
         " --input=\"#{in_dir}/#{job_id}_#{record_id}.geojson\"" +
         " --autofit=exact")
           
  # Generate erosion maps
  ['toteros','watereros','winderos'].each do |feature|
    system("DISPLAY=:0 python agmap.py" +
           " --maptype=model" +
           " --output=\"#{out_dir}/#{feature}.png\"" +
           " --input=\"#{in_dir}/#{job_id}_#{record_id}.geojson\"" +
           " --qmlfile=\"template/QMLFiles/AntaresErosion.qml\"" +
           " --featurename=#{feature}" +
           " --autofit=exact" +
           " --legendtype=#{feature}" +
           " --legendformat=png" +
           " --legendfile=#{out_dir}/#{feature}_legend.png")
  end

  # Generate sci maps
  ['sci','sciom'].each do |feature|
  system("DISPLAY=:0 python agmap.py" +
           " --maptype=model" +
           " --output=\"#{out_dir}/#{feature}.png\"" +
           " --input=\"#{in_dir}/#{job_id}_#{record_id}.geojson\"" +
           " --qmlfile=\"template/QMLFiles/Antares-SCI.qml\"" +
           " --featurename=#{feature}" +
           " --autofit=exact" +
           " --legendtype=#{feature}" +
           " --legendformat=png" +
           " --legendfile=#{out_dir}/#{feature}_legend.png")
  end

  # Generate crop budget maps, one for each year in the results
  data[:crop_year].each_with_index do |yr,n|
    system("DISPLAY=:0 python agmap.py" +
           " --maptype=budget" +
           " --output=\"#{out_dir}/budget_#{n}.png\"" +
           " --input=\"#{json_dir}/#{job_id}_#{record_id}_#{n}_cb.tif\"" +
           " --qmlfile=\"template/QMLFiles/Profitn500t500w100.qml\"")
  end

  # Generate crop budget average map
  system("DISPLAY=:0 python agmap.py" +
           " --maptype=budget" +
           " --output=\"#{out_dir}/budget_average.png\"" +
           " --input=\"#{json_dir}/#{job_id}_#{record_id}_cbaverage.tif\"" +
           " --qmlfile=\"template/QMLFiles/Profitn500t500w100.qml\"" +
           " --legendtype=profit" +
           " --legendformat=png" +
           " --legendfile=#{out_dir}/profit_legend.png")

# Generate rate of return maps
  data[:crop_year].each_with_index do |yr,n|
    system("DISPLAY=:0 python agmap.py" +
           " --maptype=budget" +
           " --output=\"#{out_dir}/rate_of_return_#{n}.png\"" +
           " --input=\"#{json_dir}/#{job_id}_#{record_id}_#{n}_rr.tif\"" +
           " --qmlfile=\"template/QMLFiles/RateOfReturn.qml\"" +
           " --legendtype=rr" +
           " --legendformat=png" +
           " --legendfile=#{out_dir}/rate_of_return_legend.png")
  end

  # Generate dem map
  #system("DISPLAY=:0 python agmap.py" +
         #" --maptype=dem" +
         #" --output=\"#{out_dir}/dem.png\"" +
         #" --input=\"#{json_dir}/#{job_id}_#{record_id}_fieldboundary.json\"" +
         #" --inputdem=\"#{json_dir}/#{job_id}_#{record_id}_dem.tif\"" +
         #" --inputchan=\"#{json_dir}/#{job_id}_#{record_id}_channel.tif\"" +
         #" --qmlfile=\"template/QMLFiles/bnd_blue_nameoutline.qml\"" +
         #" --width=2000 --height=2000 --autofit=false")
  system("DISPLAY=:0 python agmap.py" +
         " --maptype=aerial" +
         " --output=\"#{out_dir}/dem.png\"" +
         " --input=\"#{json_dir}/#{job_id}_#{record_id}_fieldboundary.json\"" +
         " --inputdem=\"#{json_dir}/#{job_id}_#{record_id}_dem.tif\"" +
         " --inputchan=\"#{json_dir}/#{job_id}_#{record_id}_channel.tif\"" +
         " --qmlfile=\"template/QMLFiles/bnd_blue_nameoutline.qml\"" +
         " --width=2000 --height=2000 --autofit=false")

  # Generate dem legend separately
  system("DISPLAY=:0 python agmap.py" +
         " --maptype=aerial" +
         " --output=\"#{out_dir}/trash_dem.png\"" +
         " --input=\"#{json_dir}/#{job_id}_#{record_id}_dem.tif\"" +
         " --inputdem=\"#{json_dir}/#{job_id}_#{record_id}_dem.tif\"" +
         " --inputchan=\"#{json_dir}/#{job_id}_#{record_id}_channel.tif\"" +
         " --legendtype=dem" +
         " --legendformat=png" +
         " --legendfile=#{out_dir}/dem_legend.png" +
         " --width=2000 --height=2000 --autofit=false")

  # Generate dem legend...right now this doesn't work along with the dem-aerial
  # overlay
  #system("DISPLAY=:0 python agmap.py" +
         #" --maptype=dem" +
         #" --output=\"#{out_dir}/trash_dem.png\"" +
         #" --input=\"#{json_dir}/#{job_id}_#{record_id}_dem.tif\"" +
         #" --legendformat=png" +
         #" --legendtype=dem" +
         #" --legendformat=png" +
         #" --legendfile=#{out_dir}/dem_legend.png" +
         #" --legendunits=ft")


  # Generate aerial map with overlay
  #system("DISPLAY=:0 python agmap.py" +
         #" --maptype=aerial" +
         #" --output=\"#{out_dir}/aerial.png\"" +
         #" --input=\"#{json_dir}/#{job_id}_#{record_id}_fieldboundary.json\"" +
         #" --qmlfile=\"template/QMLFiles/bnd_redblack_name.qml\"" +
         #" --width=2000 --height=2000 --autofit=false")
  system("DISPLAY=:0 python agmap.py" +
         " --maptype=aerial" +
         " --output=\"#{out_dir}/aerial.png\"" +
         " --input=\"#{json_dir}/#{job_id}_#{record_id}_fieldboundary.json\"" +
         " --qmlfile=\"template/QMLFiles/bnd_redblack_name.qml\"" +
         " --width=2000 --height=2000 --autofit=false")


  # Generate Soil Loss map
  system("DISPLAY=:0 python agmap.py" +
         " --maptype=dem" +
         " --output=\"#{out_dir}/soil_loss.png\"" +
         " --input=\"#{json_dir}/#{job_id}_#{record_id}_seg_soil_loss.tif\"" +
         " --legendtype=soilloss --legendformat=png" +
         " --width=2000 --height=2000 --autofit=false")

  # Generate Sediment Load map
  system("DISPLAY=:0 python agmap.py" +
         " --maptype=dem" +
         " --output=\"#{out_dir}/seg_sed_load.png\"" +
         " --input=\"#{json_dir}/#{job_id}_#{record_id}_seg_sed_load.tif\"" +
         " --legendtype=soilloss --legendformat=png" +
         " --width=2000 --height=2000 --autofit=false")
end



def self.make_histograms(data)
  job_id = data[:job_id]
  record_id = data[:record_id]

  data[:crop_year].each_with_index do |yr,n|
    command = "ruby hist.rb #{job_id}_#{record_id}_#{n}_cb.tif.aux.xml"
    system(command)
    command = "ruby hist_rr.rb #{job_id}_#{record_id}_#{n}_rr.tif.aux.xml"
    system(command)
  end

  command = "ruby hist.rb #{job_id}_#{record_id}_cbaverage.tif.aux.xml"
  system(command)
end


end # module PdfReport













################################################################################
# Run this bit if this file is being run directly as an executable rather than
# being imported as a module.
if __FILE__ == $0

#PdfReport::make_gis_images
#exit 0
require 'json'

input_file = ARGV.shift
job_id = ARGV.shift

# Input file might be a concatenated file from a message molecule, but if so,
# one of the messages is blank. We just need to remove the separator sequence
worker_data = JSON.parse(File.read(input_file).gsub!('#####!@$$@!#####',''))

#FileUtils.cp(input_file,"#{job_id}_pass_2_out")


################################################################################
#  This section contains all the symbols that are accessed by                  #
#  report.html.erb in the report template. The hash types directly following   #
#  an array describe what the array should contain. The report generator will  #
#  need to ensure that each of these variables is populated.                   #
################################################################################

data = {}    # Master hash containing all the data

data[:job_id] = ''
data[:job_id] = job_id

data[:record_id] = ''
data[:record_id] = worker_data['record_id']


data[:field_id] = ''                 # String
data[:field_id] = 'To be queried from DB'

field_data = JSON.parse(File.read("json/#{job_id}_#{data[:record_id]}_jobdetail.json"))

#db = PG.connect(
        #host: 'development-rds-pgsq.csr7bxits1yb.us-east-1.rds.amazonaws.com',
        #dbname: 'praxik',
        #user: 'app',
        #password: 'app')
#sql = "SELECT owner,field FROM isa_run1_scn WHERE uuid='#{data[:record_id]}'"
#result = db.exec( sql )
#data[:field_id] = result[0]['owner'] + ', ' + result[0]['field']

data[:field_id] = field_data['owner'] + ', ' + field_data['field']


data[:crop_rotation] = ''            # String
data[:crop_rotation] = worker_data['inputs']['rotation']

data[:dominant_critical_soil] = ''   # String
data[:dominant_critical_soil] = worker_data['musym']

data[:management_operations] = []    # Array of hash, see next
management_op = {'date' => '',             # String
                 'operation' => '',        # String
                 'crop' => '',             # String
                 'residue_cover' => 0.0}   # Float
worker_data['manop_table'].each do |man|
  r_c = man['surfcov']
  r_c = r_c.to_f if r_c.is_a?(String)
  data[:management_operations] <<
    { 'date' => man['date'],
      'operation' => man['operation'],
      'crop' => man['crop'],
      'residue_cover' => r_c
    }
end

data[:soil_details] = []             # Array of hash, see next
soil_deets = {'map_unit_symbol' => '',
              'tolerable_soil_loss' => 0.0,
              'erosion' => 0.0,
              'conditioning_index' => 0.0,
              'organic_matter_sf' => 0.0,
              'field_operation_sf' => 0.0,
              'erosion_sf' => 0.0}
data[:soil_map] = []
soil_map_entry =  {'id' => '',
                   'ac' => 0.0,
                   'pct' => 0.0,
                   'description' => ''}
#data[:soil_map] << {'id'=>'TO DO','ac'=>0.0,'pct'=>0.0,'description'=>'Need to sift through xxxx_yyyy_job.json for this data'}

data[:field_size] = 0.0
record_id = worker_data['record_id']
soils = JSON.parse(File.read("report_data/#{job_id}_#{record_id}.json"))['soil_table']
soils.each do |soil|
  data[:soil_details] << {'map_unit_symbol' => soil['musym'],
                         'domcrit' => soil['domcrit'],
                         'tolerable_soil_loss' => soil['tfact'],
                         'erosion' => soil['toteros'],
                         'conditioning_index' => soil['sci'],
                         'organic_matter_sf' => soil['sciom'],
                         'field_operation_sf' => soil['scifo'],
                         'erosion_sf' => soil['scier']}
  data[:soil_map] << {'id' => soil['musym'],
                      'domcrit' => soil['domcrit'],
                      'ac' => soil['acres'],
                      'pct' => soil['pct'],
                      'description' => soil['muname']}
  data[:field_size] += soil['acres'].to_f
end
#data[:soil_details] << soil_deets


# These control the soil properties maps that are shown on the soil_maps
# page. They will be displayed in the order added here. :label species the
# text that goes to the left of the map image. :name specifies the expected
# name of the map and legend files. 'toteros' will be expanded to 'toteros.png'
# and 'toteros_legend.png' in the generated html.
data[:soil_map_info] = []
data[:soil_map_info] << {:label=>'Total Erosion',:name=>'toteros'}
data[:soil_map_info] << {:label=>'Wind Erosion',:name=>'winderos'}
data[:soil_map_info] << {:label=>'Water Erosion',:name=>'watereros'}
data[:soil_map_info] << {:label=>'SCI',:name=>'sci'}
data[:soil_map_info] << {:label=>'SCIOM',:name=>'sciom'}


# The number of entries in the following three arrays should match
data[:crop_year] = []     # Array of strings
data[:crop] = []          # Array of strings
data[:grain_yield] = []   # Array of strings

#worker_data['inputs']['yield'].each do |yld|
field_data['yield_average'].each do |yld|
  data[:crop_year] << yld['year'].to_s
  data[:crop] << yld['crop']
  data[:grain_yield] << yld['yield']
end

data[:cb_average] = []
data[:cb_min] = []
data[:cb_max] = []
data[:cb_crop] = []
data[:cb_year] = []
data[:cb_rr_average] = []
field_data['crop_budget'].each_with_index do |budg|
  data[:cb_average] << budg['average']
  data[:cb_min] << budg['min']
  data[:cb_max] << budg['max']
  data[:cb_crop] << budg['crop']
  data[:cb_year] << budg['year']
  data[:cb_rr_average] << budg['rr_average']
end

data[:cb_multi_year_average] =
                      data[:cb_average].reduce(:+).to_f / data[:cb_average].size


data[:yield_mapunit] = field_data['yield_mapunit']
data[:yield_mapunit].each do |ymi|
  mukey = ymi['mukey']
  musym = ''
  soils.each do |soil|
    if soil['mukey'] == mukey
      musym = soil['musym']
      break
    end
  end
  ymi['musym'] = musym
end


data[:diesel_use] = 0.0
dsl = worker_data['diesel']
dsl = dsl.to_f if dsl.is_a?(String)
data[:diesel_use] = dsl
data[:water_erosion] = 0.0
data[:water_erosion] = worker_data['watereros']
data[:wind_erosion] = 0.0
data[:wind_erosion] = worker_data['winderos']
data[:soil_conditioning_index] = 0.0
data[:soil_conditioning_index] = worker_data['sci']
data[:organic_matter_sf] = 0.0
data[:organic_matter_sf] = worker_data['sciom']
data[:field_operation_sf] = 0.0
data[:field_operation_sf] = worker_data['scifo']
data[:erosion_sf] = 0.0
data[:erosion_sf] = worker_data['scier']


PdfReport::make_gis_images(data,field_data)
PdfReport::make_histograms(data)

pdfs = []
# These are the report sections which will appear in the report in the order
# specified by this array.
names = ['managements','soils','results','profit','rate_of_return','soil_maps','r2_maps']
names.each do |name|
  pdfs << PdfReport::make_pdf("template/#{name}.html.erb",'header.html',data,name)
end
report_name = "report_data/#{data[:job_id]}_#{data[:record_id]}_report.pdf"

# Stitch the report pieces into one pdf
# pdftk is faster, but creates a pdf that is larger
#system("pdftk #{pdfs.join(' ')} cat output #{report_name}")

# Ghostscript takes a bit longer, but can reduce the pdf size by factor of two!
system("gs -dBATCH -dNOPAUSE -q -sDEVICE=pdfwrite -dPDFSETTINGS=/prepress -sOutputFile=#{report_name} #{pdfs.join(' ')}")

# Reject blank pages in the pdf. Set option :y to the same number as
# .logo{max-height} from main.css for now.
RemoveBlankPages.remove(report_name,{:y=>100})

# Push file to S3
# TODO: This block needs some work:
# - error handling
# - get credentials on commandline
if File.exists?(report_name)
  credentials = YAML.load(File.read('credentials.yml'))
  s3 = AWS::S3.new(credentials)
  bucket = s3.buckets['6k_test.praxik']
  obj = bucket.objects.create("reports/#{report_name}",Pathname.new(report_name))
end

exit 0    

end

