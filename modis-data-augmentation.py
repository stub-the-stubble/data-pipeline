import requests
import os
import re
import geopandas as gpd
import fiona

#See doc here: https://firms.modaps.eosdis.nasa.gov/api/kml_fire_footprints/
TIME_PERIOD = '24h' #other options being 48h, 72h, 7days. 

#Shape files from  https://github.com/guneetnarula/indian-district-boundaries/tree/master
path = "indian-district-boundaries/topojson"
districts_file_name = 'india-districts-2019-734'

url = f"https://firms.modaps.eosdis.nasa.gov/api/kml_fire_footprints/south_asia/{TIME_PERIOD}/c6.1"

#Download the latest fire data
response = requests.get(url)
with open("local_file.kmz", "wb") as f:
    f.write(response.content)


districts_gdf = gpd.read_file(os.path.join(path, districts_file_name + '.json'))
print(districts_gdf)

#fires_kmz = 'FirespotArea_MODIS_C61_South_Asia_72h.kmz'
fires_kmz = 'local_file.kmz'
fiona.drvsupport.supported_drivers['libkml'] = 'rw' # enable KML support which is disabled by default
fiona.drvsupport.supported_drivers['LIBKML'] = 'rw' # enable KML support which is disabled by default

#fires_gdf = gpd.read_file(fires_kmz)

fires_gdf = gpd.GeoDataFrame()
for layer in fiona.listlayers(fires_kmz):    
    fgdf = gpd.read_file(fires_kmz, driver='LIBKML', layer=layer)
    for i, fire in fgdf.iterrows():
        if re.search('Centroid', fire['Name']):
            fires_gdf = fires_gdf._append(fgdf)
            break

#fires_gdf.drop(inplace = True, columns=['Name','timestamp','begin','end','altitudeMode','tessellate','extrude','visibility','drawOrder','icon'])
#
#desc_vector = fires_gdf["description"].str.replace('\s+'," ",regex=True)
#desc_vector = desc_vector.str.replace('<br/>',",", regex=True)
#desc_vector = desc_vector.str.replace('<.*?>',"", regex=True)
#desc_vector = desc_vector.str.strip()
#fires_gdf["description"] = desc_vector

print(fires_gdf)

fires_gdf['district'] = None
fires_gdf['state'] = None

for i, fire in fires_gdf.iterrows():
    for j, district in districts_gdf.iterrows():
        if fire['geometry'].within(district['geometry']):
            fires_gdf.at[i, 'district'] = district['district']
            fires_gdf.at[i, 'state'] = district['st_nm']

fires_per_state = fires_gdf.groupby('state').size().reset_index(name='fires_count')
fires_gdf.to_csv('fires_gdf.csv', index=False)
print(fires_per_state)
