require_relative './landsat_scene_finder'
require_relative './ezqlib'
require 'json'
require 'yaml'


# Test data
# =========
aoi = 'test_aoi.geojson'
cloud_cover = 20
start_date = '2014-01-01'
end_date ='2014-09-01'


AWS.config(YAML.load_file('credentials.yml'))




# This part will happen on the wep app side
# =========================================
puts "Querying for scenes..."
scenes = LsSceneFinder.find_scenes(start_date,end_date,cloud_cover,aoi)
puts "Found #{scenes.size} scenes"
# The selected scene_id will be sent to the landsat worker
scene_id_from_webapp = scenes.last[:sceneID]
puts "Choosing scene: #{scene_id_from_webapp}"

preamble = {}
ezq = {}
file = {'bucket'=>'roi.agsolver','key'=>'web_development/fields/field_Voight_North_35_1415029681.geojson'}
ezq['get_s3_files'] = [file]
preamble['EZQ'] = ezq

body = {'job_id'=>'000_test_job','scene_id'=>scene_id_from_webapp}

msg = "#{preamble.to_yaml}\n#{body.to_json}"

EZQ.enqueue_message(body.to_json,preamble,'landsat_worker',true)
