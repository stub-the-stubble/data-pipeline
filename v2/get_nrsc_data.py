import requests, argparse, json, os
from datetime import date as calendar_date
from pathlib import Path
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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
    if not args.temp:
        # Production history must never be silently bootstrapped from a sparse checkout.
        from backfill import require_histories
        require_histories(path)
    for folder in ["csv", *states]:
        Path(path, folder).mkdir(parents=True, exist_ok=True)

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

    fires_gdf = validate_source(fires_gdf, date)
    fires_gdf = filter_data(fires_gdf)

    prepared = []
    for state in states:
        districts_gdf = get_districts_geometry(state)
        state_gdf = filter_state(fires_gdf, state)
        print(f"\nIdentifying districts in {STATES_NAMES[state]}")
        state_gdf = add_district_data(state_gdf, districts_gdf)
        state_gdf = remove_nrsc_columns(state_gdf)
        state_df = pd.DataFrame(state_gdf)
        prepared.append((state_df, districts_gdf, state))

    # Validate both states before writing either state.
    for state_df, districts_gdf, state in prepared:
        write_todays_date_data(state_df, districts_gdf, state, date, path)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("date", nargs="?", default=utils.get_todays_date(), type=validate_date)
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
    return _gdf[_gdf.state.eq(_state)].copy()


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
        ],
        errors="ignore",
    )


def validate_date(value):
    try:
        parsed = calendar_date.fromisoformat(value)
        if parsed.isoformat() != value or value > utils.get_todays_date():
            raise ValueError()
        return value
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must be YYYY-MM-DD and not in the future") from exc


class DownloadFrameParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.sources = []

    def handle_starttag(self, tag, attrs):
        if tag == "iframe":
            source = dict(attrs).get("src")
            if source:
                self.sources.append(source)


def download_url(html, response_url):
    parser = DownloadFrameParser()
    parser.feed(html)
    if len(parser.sources) != 1:
        raise ValueError("NRSC response did not contain one download iframe")
    url = urljoin(response_url, parser.sources[0])
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.netloc != "bhuvan-app1.nrsc.gov.in" or not parsed.path.lower().endswith(".zip"):
        raise ValueError("NRSC returned an unexpected download URL")
    return url


def validate_source(frame, date):
    required = {"state", "sat", "detection_", "cropmask", "lat", "lon", "acqdate", "geometry"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"NRSC data is missing required columns: {sorted(missing)}")
    if frame.crs is None:
        raise ValueError("NRSC shapefile has no coordinate reference system")
    if not frame.empty:
        for field, lower, upper in [("lat", -90, 90), ("lon", -180, 180), ("detection_", 0, 100)]:
            values = pd.to_numeric(frame[field], errors="coerce")
            if not values.between(lower, upper).all():
                raise ValueError(f"NRSC returned invalid {field} values")
        if frame.state.isna().any() or not frame.sat.isin(["npp", "mod"]).all():
            raise ValueError("NRSC returned missing state or unsupported satellite values")
        dates = pd.to_datetime(frame.acqdate, errors="coerce").dt.strftime("%Y-%m-%d")
        if dates.isna().any() or not dates.eq(date).all():
            raise ValueError(f"NRSC returned observations outside requested date {date}")
        if frame.geometry.isna().any() or frame.geometry.is_empty.any():
            raise ValueError("NRSC returned missing fire geometry")
    return frame.to_crs(CRS)


def get_data_from_nrsc(date, path):
    Path(path, "csv").mkdir(parents=True, exist_ok=True)
    download_dir = Path(path, "downloads", date)
    download_dir.mkdir(parents=True, exist_ok=True)
    fires_gdf_list = []
    with requests.Session() as session:
        retry = Retry(total=2, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
        session.mount("https://", HTTPAdapter(max_retries=retry))
        for sensor in SENSORS:
            print(f"Downloading {sensor} observations for {date}...")
            response = session.get(
                "https://bhuvan-app1.nrsc.gov.in/2dresources/fire_shape/create_shapefile_v2.php",
                params={"date": date, "s": sensor, "y1": date[:4]}, timeout=(10, 30),
            )
            response.raise_for_status()
            url = download_url(response.text, response.url)
            archive = session.get(url, timeout=(10, 30))
            archive.raise_for_status()
            zip_path = download_dir / f"{sensor}.zip"
            zip_path.write_bytes(archive.content)
            shapefile = utils.get_shp_from_zip(zip_path)
            frame = gpd.read_file(f"zip://{zip_path.resolve().as_posix()}!{shapefile}")
            fires_gdf_list.append(validate_source(frame, date))
    nonempty = [frame for frame in fires_gdf_list if not frame.empty]
    combined = gpd.GeoDataFrame(pd.concat(nonempty or fires_gdf_list[:1], ignore_index=True), crs=CRS)
    combined.to_csv(Path(path, "csv", f"{date}.csv"), index=False)
    return combined


def add_district_data(_gdf, districts_gdf):
    # Create perform spatial join
    result_gdf = _gdf.sjoin(
        districts_gdf[["District", "geometry"]], how="left", predicate="within"
    )
    if len(result_gdf) != len(_gdf):
        raise ValueError("District boundaries assigned a fire more than once")
    # Retain the source state. A neighbouring polygon is not evidence to move a fire.
    result_gdf["district"] = result_gdf.District
    result_gdf["district_assignment"] = result_gdf.District.notna().map(
        {True: "polygon", False: "unassigned"}
    )
    result_gdf.drop(columns=["index_right", "District"], inplace=True)

    return result_gdf


def get_districts_geometry(state_code):
    state_name = STATES_NAMES[state_code]
    return gpd.read_file(f"{GEOJSON_PATH}/{state_name}_DISTRICT_BDY.json").set_crs(CRS)


def create_historical_data_obj(districts_gdf):
    obj = {"total": {"dates": {}}, "districts": {}}
    districts = districts_gdf.District.tolist()
    for dist in districts:
        obj["districts"][dist] = {"dates": {}}

    return obj


def write_todays_date_data(fires_df, districts_gdf, _state, _date, path):
    update_time_ist = utils.get_time_string()
    todays_data = {}
    todays_data["last_update"] = update_time_ist
    todays_data["total"] = len(fires_df)
    unassigned = int(fires_df.district.isna().sum())
    todays_data["unassigned_total"] = unassigned

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
        h_d["districts"].setdefault(district, {"dates": {}})["dates"][_date] = district_count

        # Sort the dates just in case
        dates_sorted = dict(sorted(h_d["districts"][district]["dates"].items()))
        h_d["districts"][district]["dates"] = dates_sorted

    unknown_names = set(district_counts) - set(district_names)
    if unknown_names or sum(result_json.values()) + unassigned != len(fires_df):
        raise ValueError("District counts do not reconcile with the state total")
    h_d.setdefault("unassigned", {"dates": {}})["dates"][_date] = unassigned
    h_d["unassigned"]["dates"] = dict(sorted(h_d["unassigned"]["dates"].items()))

    print("Total: ", todays_data["total"])
    print("District unassigned: ", unassigned)
    print("District wise data:")
    pprint(result_json)
    todays_data["locations"] = json.loads(fires_df.to_json(orient="records"))
    todays_data["districts"] = result_json

    print(f"\nWriting to  {path}/{_state}/{_date}.json")
    daily_json = json.dumps(todays_data, separators=(",", ":"), allow_nan=False)

    h_d["total"]["dates"][_date] = len(fires_df)

    # Sort the dates just in case
    dates_sorted = dict(sorted(h_d["total"]["dates"].items()))
    h_d["total"]["dates"] = dates_sorted

    h_d["last_update"] = update_time_ist

    print(f"Writing to {path}/{_state}/historical_data.json")
    history_json = json.dumps(h_d, allow_nan=False)
    atomic_write(Path(path, _state, f"{_date}.json"), daily_json)
    atomic_write(Path(path, _state, "historical_data.json"), history_json)


def atomic_write(path, contents):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(contents, encoding="utf-8")
    os.replace(temporary, path)


if __name__ == "__main__":
    main()
