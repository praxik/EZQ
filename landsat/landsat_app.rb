require_relative './landsat_scene_finder'
require_relative './link_scraper'
require_relative './scene_getter'
require_relative './create_images'
require_relative './ezqlib'


# Test data
# =========
aoi = 'test_aoi.geojson'
start_date = '2014-01-01'
end_date ='2014-09-01'





# This part will happen on the wep app side
# =========================================
scenes = LsSceneFinder.find_scenes(start_date,end_date,aoi)
# The selected scene_id will be sent to the landsat worker
scene_id_from_webapp = scenes.last[:sceneID]





# Landsat worker begins here
# ==========================

class LandsatWorker


  def initialize
    AWS.config(YAML.load_file('credentials.yml'))
  end



  def process_scene(scene_id,aoi_file)
    scene_file = "#{scene_id}.tar.gz"
    bucket = 'landsat.agsolver'


    # Local cache > S3 cache > hit up website.
    # This really does all that. For real.
    # And caches stuff back to S3 when needed.
    if !fetch_S3(bucket,scene_file)
      fetch_earthexplorer(bucket,scene_id,scene_file)
    end

    # Inflate the scene and make the required NDVI
    system("tar -zxvf #{scene_file}")
    NDVI.from_landsat( "./","#{scene_id}",aoi_file )

    # Do something with the generated image, and write out a result
    # message for EZQ::Processor to slurp.

    #cleanup(scene_id)
  end



  private
  def fetch_S3(bucket,file)
    if EZQ.get_s3_file(bucket,file)
      increment_S3_counter(bucket,scene_file)
      return true
    else
      return false
    end
  end



  private
  def put_S3(bucket,file)
    EZQ.send_file_to_s3(file,bucket,file)
    increment_S3_counter(bucket,file)
  end



  private
  def increment_S3_counter(bucket,file)
    count_file = 'landsat_count.json'
    if !EZQ.get_s3_file(bucket,count_file)
      # File in S3 doesn't exist or was clobbered. Make a new one.
      File.write(count_file,'{}')
    end
    counts = JSON.parse(File.read(count_file))
    current = counts.fetch(file,0)
    counts[file] = current + 1
    File.write(count_file,counts.to_json)
    EZQ.send_file_to_s3(count_file,bucket,count_file)
  end



  private
  def fetch_earthexplorer(bucket,scene_id,file)
    url = EELinkScraper(scene_id)
    SceneGetter.scene(url,file)
    put_s3(bucket,scene_file)
  end



  private
  def cleanup(scene_id)
    Dir.glob("#{scene_id}*.{tif,TIF,txt,TXT}").each{|f| File.unlink(f)}
  end


end

# This bit will be replaced by a commandline setup
# so that this script can be set up as the process_command
# for EZQ::Processor
# Penn, don't forget to invoke this as
# ruby -I . script_name.rb
# to account for the load_path needed for the leaf wrappers
LandsatWorker.new.process_scene(scene_id_from_webapp,aoi)
