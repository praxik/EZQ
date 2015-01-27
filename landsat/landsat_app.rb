require_relative './landsat_scene_finder'
require_relative './link_scraper'
require_relative './scene_getter'
require_relative './create_images'


# Test data
# =========
aoi = 'test_aoi.geojson'
start_date = '2014-01-01'
end_date ='2014-09-01'





# This part will happen on the wep app side
# =========================================
scenes = LsSceneFinder.find_scenes(start_date,end_date,aoi)
# The selected scene_id will be sent to the landsat worker
scene_id = scenes.last[:sceneID]





# Landsat worker begins here
# ==========================

# Check whether we have scene in local cache
# Increment counter in S3 metadata for this file


# Check whether we have scene in S3 cache
# Pull from S3 and increment counter in metadata


# Fetch scene from EarthExplorer if it isn't in either cache
url = EELinkScraper(scene_id)
scene_file = "#{scene_id}.tar.gz"
SceneGetter.scene(url,scene_file)
# And push the scene into S3 with a counter in the metadata


# Inflate the scene and make the required NDVI
system("tar -zxvf #{scene_file}")
NDVI.from_landsat( "./","#{scene_id}",aoi )


# Clean up the files inflated from scene
