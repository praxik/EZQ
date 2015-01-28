require 'logger'

require_relative './link_scraper'
require_relative './scene_getter'
require_relative './create_images'
require_relative './ezqlib'



class LandsatWorker


  def initialize
    @log = Logger.new(STDOUT)
    @log.level = Logger::INFO
  end


  # Processes the scene referenced by scene_id to generate NDVIs
  # related to the region specified in aoi_file
  def process_scene(scene_id,aoi_file)
    scene_file = "#{scene_id}.tar.gz"
    bucket = 'landsat.agsolver'


    # Local cache > S3 cache > hit up website.
    if !fetch_S3(scene_id,bucket,scene_file)
      fetch_earthexplorer(bucket,scene_id,scene_file)
    end


    if !Dir.exist?(scene_id)
      @log.info "Inflating scene"
      Dir.mkdir(scene_id)
      system("tar -C #{scene_id} -zxvf #{scene_file}")
    end
    NDVI.from_landsat( "#{scene_id}","#{scene_id}",aoi_file )

    # Do something with the generated images, and write out a result
    # message for EZQ::Processor to slurp.

    cleanup(scene_id,scene_file,aoi_file)
  end



  private
  def fetch_S3(scene_id,bucket,file)
    if Dir.exist?(scene_id)
      @log.info "Using scene from local cache"
      increment_S3_counter(bucket,file)
      return true
    end

    @log.info "Checking for cached scene in S3"
    if EZQ.get_s3_file(bucket,file)
      @log.info "Using scene from S3 cache"
      increment_S3_counter(bucket,file)
      return true
    else
      return false
    end
  end



  private
  def put_S3(bucket,file)
    @log.info "Caching scene in S3"
    EZQ.send_file_to_s3(file,bucket,file)
    increment_S3_counter(bucket,file)
  end



  private
  def increment_S3_counter(bucket,file)
    @log.info "Incrementing use count for #{file}"
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
    @log.info "Getting scene from EE"
    url = EELinkScraper.scrape(scene_id)
    SceneGetter.scene(url,file)
    put_S3(bucket,file)
  end



  private
  def rm_if_exist(f)
    FileUtils.rm(f) if File.exist?(f)
  end


  private
  def cleanup(scene_id,scene_file,aoi_file)
    rm_if_exist(scene_file)
    #rm_if_exist(aoi_file)
  end


end
