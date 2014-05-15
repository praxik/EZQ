require 'erb'
require 'pdfkit'

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
def self.make_pdf(template,header,data)
  Dir.chdir(File.dirname(template))
  html_in = File.basename(template)
  input = File.read(html_in)

  header = File.read(header)

  erbed = ERB.new(input)

  File.write('report.html',erbed.result)

  pdfkit = PDFKit.new(File.new('report.html'),
                    #:print_media_type => true,
                    :page_size => 'Letter',
                    :margin_left => '2mm',
                    :margin_right => '2mm',
                    :margin_top => '35mm',
                    :margin_bottom => '10mm',
                    :footer_center => 'Page [page] of [topage]',
                    :header_html => 'header.html'
                    )

  pdfkit.to_file("report_data/#{data[:job_id]}_#{data[:record_id]}_report.pdf")
end



def self.make_gis_images(data)

  soil_mapper = "Soil.py"
  budget_mapper = "Budget.py"

  in_dir = "report_data"
  out_dir = "report_data"

  job_id = data[:job_id]
  rec_id = data[:record_id]

  soil_file = "#{in_dir}/#{job_id}_#{record_id}.geojson"

  common = " --width=2000" +
           " --height=2000" +
           " --scale=1.08" +
           " --label=Horizontal" +
           " --labelSize=150"

  # Generate musym map
  system("DISPLAY=:0 python #{soil_mapper}" +
           " --output=\"#{out_dir}/musym.png\"" +
           " --featureName=musym" +
           " --input=\"#{in_dir}/#{job_id}_#{record_id}.geojson\"" +
           " --mapType=categorized" +
           " --textLabel=True" +
           "#{common}")
           
  # Generate erosion maps
  ['toeros','wateros','winderos'].each do |feature|
    system("DISPLAY=:0 python #{soil_mapper}" +
           " --output=\"#{out_dir}/#{feature}.png\"" +
           " --featureName=#{feature}" +
           " --input=\"#{in_dir}/#{job_id}_#{record_id}.geojson\"" +
           " --mapType=QML" +
           " --QMLFile=\"template/QMLFiles/AntaresErosion.qml\"" +
           "#{common}")
  end

  # Generate sci maps
  ['sci','sciom'].each do |feature|
    system("DISPLAY=:0 python #{soil_mapper}" +
           " --output=\"#{out_dir}/#{feature}.png\"" +
           " --featureName=#{feature}" +
           " --input=\"#{in_dir}/#{job_id}_#{record_id}.geojson\"" +
           " --mapType=QML" +
           " --QMLFile=\"template/QMLFiles/Antares-SCI.qml\"" +
           "#{common}")
  end

  # Generate crop budget maps, one for each year in the reaults
  data[:crop_year].each_with_index do |yr,n|
    system("DISPLAY=:0 python #{budget_mapper}" +
    " --output=\"#{out_dir}/budget_#{n}.png\"" +
    " --input=\"#{in_dir}/#{job_id}_#{record_id}_#{n}_cb.tif\"" +
    " --mapType=test" +
    " --QMLFile=\"template/QMLFiles/Profitn500t500w100.qml\"" +
    "#{common}")
  end

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
worker_data = JSON.parse(File.read(input_file))


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
end
#data[:soil_details] << soil_deets


# The number of entries in the following three arrays should match
data[:crop_year] = []     # Array of strings
data[:crop] = []          # Array of strings
data[:grain_yield] = []   # Array of strings

worker_data['inputs']['yield'].each do |yld|
  data[:crop_year] << yld['year'].to_s
  data[:crop] << yld['crop']
  data[:grain_yield] << yld['value']
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


PdfReport::make_gis_images(data)
PdfReport::make_pdf('template/report.html.erb','header.html',data)

end

