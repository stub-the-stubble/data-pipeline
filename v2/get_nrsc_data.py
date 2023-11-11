import requests, argparse, json, os, re
import geopandas as gpd
import pandas as pd
from pprint import pprint
from constants import *
import utils
from shapely.geometry import Point
from collections import defaultdict


def main():
    args = parse_arguments()
    date = get_date(args)
    states = ["PB", "HR"]

    if not args.csv:
        print(f"Getting data for {date} from nrsc")
        fires_gdf = get_data_from_nrsc(date)
    else:
        print(f"Getting data for {date} from csv")
        fires_df = pd.read_csv(f"{CSV_PATH}/{date}.csv")
        fires_gdf = gpd.GeoDataFrame(
            fires_df,
            geometry=gpd.GeoSeries.from_wkt(fires_df["geometry"], crs="EPSG:4326"),
        )

    fires_gdf = filter_nrsc_data_cropmask(fires_gdf)

    for state in states:
        districts_gdf = get_districts_geometry(state)
        state_gdf = filter_nrsc_data_state(fires_gdf, state)
        print(f"Identifying districts in {state}")
        state_gdf = add_district_data(state_gdf, districts_gdf)
        state_gdf = filter_nrsc_data_columns(state_gdf)
        state_df = pd.DataFrame(state_gdf)
        print(state_df)
        write_todays_date_data(state_df, districts_gdf, state, date)


def get_date(args):
    if args.date:
        return args.date
    else:
        return utils.get_todays_date()


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--date")
    parser.add_argument("-c", "--csv", action="store_true")
    args = parser.parse_args()

    return args


def filter_nrsc_data_state(_gdf, _state):
    return _gdf[_gdf.state.str.contains(_state)]


def filter_nrsc_data_cropmask(_gdf):
    return _gdf[_gdf.cropmask.notna()]


def filter_nrsc_data_detection(_gdf, _sensor):
    if _sensor == "modis":
        return _gdf[_gdf.detection_ > 30]
    else:
        return _gdf[_gdf.detection_ > 7]


def filter_nrsc_data_columns(_gdf):
    return _gdf.drop(
        columns=[
            "brightness",
            "acqdate",
            "acqtime",
            "sensor",
            "cropmask",
            "geometry",
            "scanpixel_",
            "trackpixel",
            "mailsent",
            "mailsent_t",
            "village_na",
            "id",
            "orbitno",
            "coverage_f",
        ]
    )


def get_data_from_nrsc(date):
    # TODO add exponential breakoff time to session request
    s = requests.Session()
    fires_gdf_list = []

    for SENSOR in SENSORS:
        print(f"Connecting to {SENSOR} API...")

        res = s.get(
            f"https://bhuvan-app1.nrsc.gov.in/2dresources/fire_shape/create_shapefile_v2.php?date={date}&s={SENSOR}&y1=2023",
            timeout=(10, 15),
        )

        # Grab iframe src=<url> from the response text
        url = re.search(r'(?<=src=").*?(?=[\*"])', res.text)
        zipfile_name = f"shapefile_{SENSOR}.zip"

        print(f"Downloading {SENSOR} shapefile...")
        res_zip = s.get(url[0], timeout=(10, 15))
        with open(zipfile_name, "wb") as fd:
            for chunk in res_zip.iter_content(chunk_size=128):
                fd.write(chunk)
        print("Done")

        print(f"Downloading {SENSOR} shapefile...")
        shapefile = utils.get_shp_from_zip(zipfile_name)
        _gdf = gpd.read_file(f"zip:///{os.path.abspath(zipfile_name)}!{shapefile}")
        _gdf = filter_nrsc_data_detection(_gdf, SENSOR)

        fires_gdf_list.append(_gdf)

    combined_sensors_df = pd.concat(fires_gdf_list, ignore_index=True)
    combined_sensors_df.to_csv(f"{CSV_PATH}/{date}.csv", index=False)
    return combined_sensors_df


def add_district_data(_gdf, districts_gdf):
    # Create perform spatial join
    result_gdf = _gdf.sjoin(
        districts_gdf[["District", "geometry"]], how="left", predicate="within"
    )
    result_gdf["district"] = result_gdf["District"]
    result_gdf.drop(columns=["index_right", "District"], inplace=True)

    return result_gdf


def get_districts_geometry(state_code):
    state_name = STATES_NAMES[state_code]
    return gpd.read_file(f"{GEOJSON_PATH}/{state_name}_DISTRICT_BDY.json").set_crs(
        "EPSG:4326"
    )


def create_historical_data_file(state, districts_gdf):
    obj = {"total": {"dates": {}}, "districts": {}}
    districts = districts_gdf["District"].tolist()
    for dist in districts:
        obj["districts"][dist] = {"dates": {}}

    return obj


def write_todays_date_data(fires_df, districts_gdf, _state, _date):
    update_time_ist = utils.get_time_string()
    todays_data = {}
    todays_data["last_update"] = update_time_ist
    todays_data = {}
    todays_data["total"] = len(fires_df)

    districts = fires_df["district"].value_counts().to_dict()
    # Select the column values from df1
    district_names = districts_gdf["District"].unique()

    try:
        with open(f"docs/{API_VERSION}/{_state}/historical_data.json") as f:
            d = json.load(f)
    except FileNotFoundError:
        d = create_historical_data_file(_state, districts_gdf)

    result_json = {}

    for value in district_names:
        district_count = districts.get(value, 0)
        result_json[str(value)] = district_count
        d["districts"][value]["dates"][_date] = district_count

    print("District wise data")
    pprint(result_json)
    todays_data["locations"] = json.loads(fires_df.to_json(orient="records"))
    todays_data["districts"] = result_json

    print(f"Writing to  {_state}/{_date}.json")
    with open(f"docs/{API_VERSION}/{_state}/{_date}.json", "w") as outfile:
        json.dump(todays_data, outfile, separators=(",", ":"))

    d["total"]["dates"][_date] = len(fires_df)
    d["last_update"] = update_time_ist

    print(f"Writing to {_state}/historical_data.json")
    with open(f"docs/{API_VERSION}/{_state}/historical_data.json", "w") as outfile:
        outfile.write(json.dumps(d))


if __name__ == "__main__":
    main()
