# --- Leaf Modules --- #
require 'leaf_wrapper_gis_landsat_ruby'

include Leaf_wrapper_gis_landsat_ruby

a = '/home/ubuntu/dev/deps/LC81050112014233LGN00';
b = 'LC81050112014233LGN00';
c = 'field_Voight_10_1415115678.geojson';
CreateNDVIFromLandsat( a, b, c );