import requests
import os
import re
import geopandas as gpd
import fiona
from shapely.geometry import Point
from dotenv import load_dotenv

load_dotenv()
MAP_KEY = os.getenv('MAP_KEY')

#See doc here: https://firms.modaps.eosdis.nasa.gov/api/kml_fire_footprints/
TIME_PERIOD = '24h' #other options being 48h, 72h, 7days. 

#Shape files from  https://github.com/guneetnarula/indian-district-boundaries/tree/master
path = "indian-district-boundaries/topojson"
districts_file_name = 'india-districts-2019-734'

#url = f"https://firms.modaps.eosdis.nasa.gov/api/kml_fire_footprints/south_asia/{TIME_PERIOD}/c6.1"
#url = f"https://firms.modaps.eosdis.nasa.gov/api/kml_fire_footprints/south_asia/{TIME_PERIOD}/suomi-npp-viirs-c2"

#See doc here: https://firms.modaps.eosdis.nasa.gov/api/country/
#url = f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAP_KEY}/MODIS_NRT/IND/1"
url = f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAP_KEY}/VIIRS_SNPP_NRT/IND/1"
#url = f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAP_KEY}/VIIRS_NOAA20_NRT/IND/1"


#Download the latest fire data
response = requests.get(url)
with open("local_file.csv", "wb") as f:
    f.write(response.content)

districts_gdf = gpd.read_file(os.path.join(path, districts_file_name + '.json'))
fires_gdf = gpd.read_file("local_file.csv")

fires_gdf['district'] = None
fires_gdf['state'] = None

geometry = [Point(xy) for xy in zip(fires_gdf['longitude'], fires_gdf['latitude'])]

fires_gdf['geometry'] = geometry
for i, fire in fires_gdf.iterrows():
    for j, district in districts_gdf.iterrows():
        if fire['geometry'].within(district['geometry']):
            fires_gdf.at[i, 'district'] = district['district']
            fires_gdf.at[i, 'state'] = district['st_nm']

fires_per_state = fires_gdf.groupby('state').size().reset_index(name='fires_count')

fires_gdf.drop(inplace=True,columns=['geometry','country_id'])
fires_gdf.to_csv('fires_gdf.csv', index=False)

print(fires_per_state)
