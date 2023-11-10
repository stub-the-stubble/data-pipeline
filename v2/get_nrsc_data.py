import requests, argparse, json, os, re
import geopandas as gpd
import pandas as pd
from pprint import pprint
from datetime import datetime
from zoneinfo import ZoneInfo

from shapely.geometry import Point

SENSORS = ["modis","vf375"]
API_VERSION = "v2"

def main():

    args = parse_arguments()
    DATE = get_date(args)
    STATE = "PB"

    if (not args.csv):
        print(f"Getting data for {DATE} from nrsc")
        fires_gdf = get_data_from_nrsc(DATE)
    else:
        print(f"Getting data for {DATE} from csv")
        fires_gdf = pd.read_csv(f'docs/v2/csv/{DATE}.csv')

    districts_gdf = get_districts_geometry(STATE)
    print("Identifying districts")
    fires_gdf = fires_gdf.pipe(filter_nrsc_data_state, STATE)\
                         .pipe(filter_nrsc_data_cropmask)\
                         .pipe(add_district_data, districts_gdf)\
                         .pipe(filter_nrsc_data_columns)
    print(fires_gdf)
    fires_df = pd.DataFrame(fires_gdf)
    write_todays_date_data(fires_df, districts_gdf, STATE, DATE)

def get_date(args):
    if (args.date):
        return args.date
    else:
        return datetime.now(tz=ZoneInfo("Asia/Kolkata")).strftime('%Y-%m-%d')

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d","--date")
    parser.add_argument('-c', '--csv', action='store_true')
    args = parser.parse_args()

    return args

def filter_nrsc_data_state(_gdf, _state):
    return _gdf[_gdf.state.str.contains(_state)]

def filter_nrsc_data_cropmask(_gdf):
    return _gdf[_gdf.cropmask.notna()]

def filter_nrsc_data_detection(_gdf, _sensor):
    if (_sensor == "modis"):
         return _gdf[_gdf.detection_ > 30]
    else:
         return _gdf[_gdf.detection_ > 7]

def filter_nrsc_data_columns(_gdf):
   return _gdf.drop(columns=['brightness','acqdate','acqtime','sensor','cropmask','geometry','scanpixel_','trackpixel','mailsent','mailsent_t','village_na','id','orbitno','coverage_f'])

def get_data_from_nrsc(date):
    s = requests.Session()
    fires_gdf_array = []

    for SENSOR in SENSORS:
        print(f'Connecting to {SENSOR} API...')

        req = s.get(f"https://bhuvan-app1.nrsc.gov.in/2dresources/fire_shape/create_shapefile_v2.php?date={date}&s={SENSOR}&y1=2023",timeout=(10,15))
        url = re.search(r'(?<=src=").*?(?=[\*"])',req.text)
        filename = re.search(r'[^\/]+(?=\.[^\/.]*$)',url[0])
        zipfile_name = f"shapefile_{SENSOR}.zip"

        print(f'Downloading {SENSOR} shapefile...')

        req_zip = s.get(url[0],timeout=(10,15))
        with open(zipfile_name, 'wb') as fd:
            for chunk in req_zip.iter_content(chunk_size=128):
                fd.write(chunk)

        print("Done")

        _gdf = gpd.read_file(f"zip:///{os.path.realpath(zipfile_name)}!shape/{filename[0]}.shp")
        filter_nrsc_data_detection(_gdf, SENSOR)

        #_gdf.to_csv(f'fires_temp_{SENSOR}.csv', index=False)

        fires_gdf_array.append(_gdf)

    combined_sensors_df = pd.concat([fires_gdf_array[0],fires_gdf_array[1]])
    combined_sensors_df.to_csv(f'docs/v2/csv/{date}.csv', index=False)
    return combined_sensors_df


def add_district_data(_gdf, districts_gdf):

    _gdf['district'] = None
    geometry = [Point(xy) for xy in zip(_gdf['lon'], _gdf['lat'])]
    _gdf['geometry'] = geometry
    for i, fire in _gdf.iterrows():
        for j, district in districts_gdf.iterrows():
            if fire['geometry'].within(district['geometry']):
                _gdf.at[i, 'district'] = district['District']

    _gdf = _gdf[_gdf.district.notna()]
    return _gdf

def get_districts_geometry(_state):
    if (_state == 'PB'):
        return gpd.read_file("PUNJAB_DISTRICT_BDY.json")


def write_todays_date_data(fires_df, districts_gdf, _state, _date):

    todays_data = {}
    todays_data["last_update"] = datetime.now(tz=ZoneInfo("Asia/Kolkata")).strftime('%-I:%M %p, %d %b %Y')
    todays_data[_state] = {}
    todays_data[_state]["total"] = len(fires_df)


    districts = fires_df['district'].value_counts().to_dict()
    # Select the column values from df1
    district_names = districts_gdf['District'].unique()

    with open(f'docs/{API_VERSION}/historical_data.json') as f:
        d = json.load(f)
    result_json = {}

    for value in district_names:
        district_count = districts.get(value, 0)
        result_json[str(value)] = district_count
        d[_state]["districts"][value]["dates"][_date] = district_count

    print("District wise data")
    pprint(result_json)
    todays_data[_state]["locations"] = json.loads(fires_df.to_json(orient="records"))
    todays_data[_state]["districts"] = result_json

    print(f"Writing to {_date}.json")
    with open(f'docs/{API_VERSION}/{_date}.json', 'w') as outfile:
        json.dump(todays_data, outfile, separators=(',', ':'))


    d[_state]["total"]["dates"][_date]=len(fires_df)
    json_object = json.dumps(d)

    print(f"Writing to historical_data.json")
    with open(f'docs/{API_VERSION}/historical_data.json', 'w') as outfile:
        outfile.write(json_object)

if __name__ == "__main__":
    main()
