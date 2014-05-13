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

  pdfkit.to_file('report.pdf')
end



def self.make_gis_images

  scriptDir = "."#"/home/penn/vmshare/dev/leaf-apps/src/utils/gis/qgis/scripts"
  baseDir = "."#"/home/penn/vmshare/dev/leaf-apps/src/sandbox/qgiscpp_test"

  #path to python executable for generating png from json data file
  exePath = scriptDir + "/PythonImageRenderer.py"

  #path to sci input data file
  #inputSci = baseDir + "/test_data/test_attribute.geojson"

  #path to eros input data file
  #inputEros = baseDir + "/test_data/test_attribute.geojson"

  #path to musym input data file
  inputMusym = baseDir + "/test_data/fieldsWithData_INL_Name__Field 1.shp"

  #path for creating sci image
  #sciPngPath = baseDir + "/sci.png"

  #path for creating eros image
  #erosPngPath = baseDir + "/eros.png"

  #path for creating musym image
  musymPngPath = baseDir + "/musym.png"


  #test call to python executable to create png from json input data
  #sciArgs = []
  #erosArgs = []
  musymArgs = []

  #sciArgs << "--featureName=Profit"
  #sciArgs << "--mapType=graduated"
  #sciArgs << "--gradCatNum=12"
  #sciArgs << "--width=400"
  #sciArgs << "--height=400"
  #sciArgs << "--labelSize=30"
  #sciArgs << "--showLabel=False"

  #erosArgs << "--featureName=Profit"
  #erosArgs << "--mapType=graduated"
  #erosArgs << "--gradCatNum=12"
  #erosArgs << "--width=400"
  #erosArgs << "--height=400"
  #erosArgs << "--labelSize=30"
  #erosArgs << "--showLabel=False"

  musymArgs << "--featureName=CG2P_SCI"
  musymArgs << "--mapType=QML"
  musymArgs << "--width=2000"
  musymArgs << "--height=2000"
  musymArgs << "--labelSize=150"
  musymArgs << "--label=Horizontal"
  musumArgs << "--QMLFile=#{baseDir}/test_data/Antares-SCI.qml"
  musymArgs << "--scale=1.08"
  musymArgs << "--VLayerTransparency=0"
  

  #sciArgs << "--input=#{inputSci}"
  #sciArgs << "--output=#{sciPngPath}"

  #erosArgs << "--input=#{inputSci}"
  #erosArgs << "--output=#{erosPngPath}"

  musymArgs << "--input=#{inputMusym}"
  musymArgs << "--output=#{musymPngPath}"

  #sciArgs.insert(0,exePath)
  #sciArgs.insert(0,"python")

  #erosArgs.insert(0,exePath)
  #erosArgs.insert(0,"python")

  musymArgs.insert(0,exePath)
  musymArgs.insert(0,"python")

  #puts musymArgs.join(" ")
  #exit 0

  #IO.popen(sciArgs)
  #IO.popen(erosArgs)
  IO.popen(musymArgs)

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
ids = JSON.parse(File.read(input_file))


################################################################################
#  This section contains all the symbols that are accessed by                  #
#  report.html.erb in the report template. The hash types directly following   #
#  an array describe what the array should contain. The report generator will  #
#  need to ensure that each of these variables is populated.                   #
################################################################################

data = {}    # Master hash containing all the data

data[:field_id] = ''                 # String
data[:crop_rotation] = ''            # String
data[:dominant_critical_soil] = ''   # String

data[:management_operations] = []    # Array of hash, see next
management_op = {'date' => '',             # String
                 'operation' => '',        # String
                 'crop' => '',             # String
                 'residue_cover' => 0.0}   # Float

data[:soil_details] = []             # Array of hash, see next
soil_deets = {'map_unit_symbol' => '',
              'tolerable_soil_loss' => 0.0,
              'erosion' => 0.0,
              'conditioning_index' => 0.0,
              'organic_matter_sf' => 0.0,
              'field_operation_sf' => 0.0,
              'erosion_sf' => 0.0}

# The number of entries in the following three arrays should match
data[:crop_year] = []     # Array of strings
data[:crop] = []          # Array of strings
data[:grain_yield] = []   # Array of strings

data[:soil_map] = []
soil_map_entry =  {'id' => '',
                   'ac' => 0.0,
                   'pct' => 0.0,
                   'description' => ''}

data[:diesel_use] = 0.0
data[:water_erosion] = 0.0
data[:wind_erosion] = 0.0
data[:soil_conditioning_index] = 0.0
data[:organic_matter_sf] = 0.0
data[:field_operation_sf] = 0.0
data[:erosion_sf] = 0.0


################################################################################
# This section contains a test set of data for report.html.erb                 #
################################################################################

man_data = <<MAN_DATA_END
10/07/2013;Harvest, killing crop 50pct standing stubble;;83.7
10/11/2013;Shredder, flail or rotary_35% flattening;;66.0
10/11/2013;Bale corn stover_60% Flat Res Removed;;66.0
10/21/2013;Subsoil disk ripper;;33.8
04/23/2014;Cultivator, field 6-12 in sweeps;;26.8
04/25/2014;Planter, double disk opnr w/fluted coulter;;26.9
05/15/2014;Fert applic. surface broadcast;;24.2
10/07/2014;Harvest, killing crop 50pct standing stubble;;83.1
10/11/2014;Shredder, flail or rotary_35% flattening;;65.5
10/11/2014;Bale corn stover_60% Flat Res Removed;;65.5
10/21/2014;Subsoil disk ripper;;33.5
04/23/2015;Cultivator, field 6-12 in sweeps;;26.3
04/25/2015;Planter, double disk opnr w/fluted coulter;;26.4
05/15/2015;Fert applic. surface broadcast;;23.7
10/07/2015;Harvest, killing crop 50pct standing stubble;;83.0
10/11/2015;Shredder, flail or rotary_35% flattening;;65.5
10/11/2015;Bale corn stover_60% Flat Res Removed;;65.5
10/21/2015;Subsoil disk ripper;;33.4
04/23/2016;Cultivator, field 6-12 in sweeps;;33.4
04/25/2016;Planter, double disk opnr w/fluted coulter;;26.3
05/15/2016;Fert applic. surface broadcast;;23.7
10/07/2016;Harvest, killing crop 50pct standing stubble;;83.0
10/21/2016;Subsoil disk ripper;;49.7
MAN_DATA_END

soil_details_input = <<SDI_END
308B 3 2.51 0.23 0.43 0.14 0.01
203 3 0.71 0.37 0.43 0.14 0.72
259 3 0.70 0.51 0.78 0.14 0.72
308 3 0.72 0.37 0.43 0.14 0.72
823B 3 1.26 0.33 0.43 0.14 0.50
135 5 0.70 0.51 0.78 0.14 0.72
SDI_END

data[:field_id] = "Doug's Bottomland Corn Field #78"
data[:crop_rotation] = 'Corn-Corn-Beans'
data[:dominant_critical_soil] = 'Wadena loan, 32-40 inches to sand and gravel, 2-5 percent slopes (308B)'

man_data.split("\n").each do |line|
  items= line.split(';')
  data[:management_operations].push({'date'=>items[0],
                              'operation'=>items[1],
                              'crop'=>items[2],
                              'residue_cover'=>items[3]})
end

data[:management_operations] += data[:management_operations]


soil_details_input.split("\n").each do |line|
  items = line.split(' ')
  data[:soil_details].push({'map_unit_symbol'=>items[0],
                     'tolerable_soil_loss'=>items[1].to_f,
                     'erosion'=>items[2].to_f,
                     'conditioning_index'=>items[3].to_f,
                     'organic_matter_sf'=>items[4].to_f,
                     'field_operation_sf'=>items[5].to_f,
                     'erosion_sf'=>items[6].to_f})
end


data[:crop_year] << '2013' << '2014' << '2015' << '2016'

data[:crop] << 'Corn' << 'Corn' << 'Corn' << 'Corn'

data[:grain_yield] << 77 << 77 << 77 << 77

data[:diesel_use] = 5.47

data[:water_erosion] = 2.41

data[:wind_erosion] = 1.34

soil = {'id'=>'203B','ac'=>26.7,'pct'=>99.9,'description'=> <<END
A: 107 (Spillville, channeled-Coland,
channeled-Aquolls, ponded, complex, 0 to
2 percent slopes, frequently flooded)
Counties below: 83 (Biscay clay loam,
ponded, 32 to 40 inches to sand and
gravel, 0 to 1 percent slopes
END
}

4.times {data[:soil_map] << soil}


PdfReport::make_pdf('template/report.html.erb','header.html',data)

end

