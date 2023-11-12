import requests, argparse, json, os, re
import geopandas as gpd
import pandas as pd
from pprint import pprint
from constants import *
import utils


def main():
    args = parse_arguments()
    date = args.date
    path = utils.get_save_path(args.temp)
    states = ["PB", "HR"]

    if not args.csv:
        print(f"Getting data for {date} from nrsc")
        fires_gdf = get_data_from_nrsc(date, path)
    else:
        print(f"Getting data for {date} from csv")
        fires_df = pd.read_csv(f"{path}/csv/{date}.csv")
        fires_gdf = gpd.GeoDataFrame(
            fires_df,
            geometry=gpd.GeoSeries.from_wkt(fires_df["geometry"], crs=CRS),
        )

    fires_gdf = filter_data(fires_gdf)

    for state in states:
        districts_gdf = get_districts_geometry(state)
        state_gdf = filter_state(fires_gdf, state)
        print(f"\nIdentifying districts in {STATES_NAMES[state]}")
        state_gdf = add_district_data(state_gdf, districts_gdf)
        state_gdf = remove_nrsc_columns(state_gdf)
        state_df = pd.DataFrame(state_gdf)
        write_todays_date_data(state_df, districts_gdf, state, date, path)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("date", nargs="?", default=utils.get_todays_date())
    parser.add_argument(
        "-c",
        "--csv",
        action="store_true",
        default=False,
        help="read data from csv files",
    )
    parser.add_argument(
        "-t",
        "--temp",
        action="store_true",
        default=False,
        help="store data in temp directory (for testing purposes)",
    )
    args = parser.parse_args()

    return args


def filter_state(_gdf, _state):
    return _gdf[_gdf.state.str.contains(_state)]


def filter_data(_gdf):
    # Remove datapoints which do not correspond to crop areas
    _gdf = _gdf[_gdf.cropmask.notna()]

    # Remove datapoints which do not confrom to CAQM protocol
    _gdf = _gdf[
        (_gdf.sat == "npp") & (_gdf.detection_ > 7)
        | (_gdf.sat == "mod") & (_gdf.detection_ > 30)
    ]

    return _gdf


def remove_nrsc_columns(_gdf):
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


def get_data_from_nrsc(date, path):
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

        shapefile = utils.get_shp_from_zip(zipfile_name)
        _gdf = gpd.read_file(f"zip:///{os.path.abspath(zipfile_name)}!{shapefile}")

        fires_gdf_list.append(_gdf)

    combined_sensors_df = pd.concat(fires_gdf_list, ignore_index=True)
    combined_sensors_df.to_csv(f"{path}/csv/{date}.csv", index=False)

    return combined_sensors_df


def add_district_data(_gdf, districts_gdf):
    # Create perform spatial join
    result_gdf = _gdf.sjoin(
        districts_gdf[["District", "geometry"]], how="left", predicate="within"
    )
    result_gdf.district = result_gdf.District
    result_gdf.drop(columns=["index_right", "District"], inplace=True)

    return result_gdf


def get_districts_geometry(state_code):
    state_name = STATES_NAMES[state_code]
    return gpd.read_file(f"{GEOJSON_PATH}/{state_name}_DISTRICT_BDY.json").set_crs(CRS)


def create_historical_data_obj(districts_gdf):
    obj = {"total": {"dates": {}}, "districts": {}}
    districts = districts_gdf.District.tolist()
    for dist in districts:
        obj.districts[dist] = {"dates": {}}

    return obj


def write_todays_date_data(fires_df, districts_gdf, _state, _date, path):
    update_time_ist = utils.get_time_string()
    todays_data = {}
    todays_data["last_update"] = update_time_ist
    todays_data["total"] = len(fires_df)

    try:
        with open(f"{path}/{_state}/historical_data.json") as f:
            h_d = json.load(f)
    except FileNotFoundError:
        h_d = create_historical_data_obj(districts_gdf)

    # Get count of all fires in each district
    district_counts = fires_df.district.value_counts().to_dict()

    # Get names of all district
    district_names = districts_gdf.District.unique()
    result_json = {}

    for district in district_names:
        # Set count as 0 if key doesn't exist
        district_count = district_counts.get(district, 0)
        # Set today's date district-wise counts
        result_json[str(district)] = district_count
        # Set historical data for this district
        h_d["districts"][district]["dates"][_date] = district_count

    print("Total: ", todays_data["total"])
    print("District wise data:")
    pprint(result_json)
    todays_data["locations"] = json.loads(fires_df.to_json(orient="records"))
    todays_data["districts"] = result_json

    print(f"\nWriting to  {path}/{_state}/{_date}.json")
    with open(f"{path}/{_state}/{_date}.json", "w") as outfile:
        json.dump(todays_data, outfile, separators=(",", ":"))

    h_d["total"]["dates"][_date] = len(fires_df)
    h_d["last_update"] = update_time_ist

    print(f"Writing to {path}/{_state}/historical_data.json")
    with open(f"{path}/{_state}/historical_data.json", "w") as outfile:
        outfile.write(json.dumps(h_d))


if __name__ == "__main__":
    main()
