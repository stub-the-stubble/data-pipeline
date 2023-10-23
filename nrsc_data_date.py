import requests
import os
import re
import zipfile
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

# Shape files from  https://onlinemaps.surveyofindia.gov.in/Digital_Product_Show.aspx
# Restrict to punjab only for optimization

path = "indian-district-boundaries/"
districts_file_name = "PUNJAB_DISTRICT_BDY.json"
districts_gdf = gpd.read_file(os.path.join(path, districts_file_name))

DATE = "2023-10-23"
SENSORS = ["modis","vf375"]

fires_gdf_array = []
for SENSOR in SENSORS:

    r = requests.get(f"https://bhuvan-app1.nrsc.gov.in/2dresources/fire_shape/create_shapefile_v2.php?date={DATE}&s={SENSOR}&y1=2023")

    url = re.search(r'(?<=src=").*?(?=[\*"])',r.text)
    filename = re.search(r'[^\/]+(?=\.[^\/.]*$)',url[0])
    zipfile_name = f"shapefile_{SENSOR}.zip"

    print(f'Downloading {SENSOR} shapefile...')
    rs = requests.get(url[0])
    with open(zipfile_name, 'wb') as fd:
        for chunk in rs.iter_content(chunk_size=128):
            fd.write(chunk)
    print("Done")

    _gdf = gpd.read_file("zip:///"+os.path.realpath(zipfile_name)+"!shape/"+filename[0]+".shp")
    _gdf = _gdf[(_gdf.state.str.contains('PB')) & (_gdf.cropmask.notna())]
    _gdf.to_csv(f'fires_temp_{SENSOR}.csv', index=False)
    if (SENSOR == "modis"):
        _gdf = _gdf[_gdf.detection_ > 30]
    else:
        _gdf = _gdf[_gdf.detection_ > 7]

    fires_gdf_array.append(_gdf)

#
fires_gdf = pd.concat([fires_gdf_array[0],fires_gdf_array[1]])
fires_gdf['district'] = None
geometry = [Point(xy) for xy in zip(fires_gdf['lon'], fires_gdf['lat'])]
#
fires_gdf['geometry'] = geometry
for i, fire in fires_gdf.iterrows():
    for j, district in districts_gdf.iterrows():
        if fire['geometry'].within(district['geometry']):
            fires_gdf.at[i, 'district'] = district['District']
#
fires_gdf.drop(inplace=True,columns=['detection_','brightness','cropmask','geometry','scanpixel_','trackpixel','mailsent','mailsent_t','village_na','id','orbitno','coverage_f'])
fires_gdf = fires_gdf[fires_gdf.district.notna()]
print(fires_gdf)
fires_gdf.to_json('data.json', orient="records")
