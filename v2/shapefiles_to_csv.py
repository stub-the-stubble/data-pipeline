import geopandas as gpd
import pandas as pd
import os, argparse
import utils
from constants import *


def main():
    args = parse_arguments()
    gdf = read_shapefiles(args.files)
    print(gdf.dtypes)
    dataframes = split_into_dates(gdf, "acqdate")

    path = utils.get_save_path(args.temp)
    write_to_dates_csv(dataframes, path)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+", help="List of file paths")
    parser.add_argument("-t", "--temp", action="store_true", default=False)

    args = parser.parse_args()
    return args


def read_shapefiles(zipfiles):
    gdf_list = []
    for zipfile in zipfiles:
        shape_path = utils.get_shp_from_zip(zipfile)
        gdf = gpd.read_file(f"zip:///{os.path.abspath(zipfile)}!{shape_path}")
        gdf_list.append(gdf)

    combined_df = pd.concat(gdf_list, ignore_index=True)
    return combined_df


def split_into_dates(combined_gdf, date_col):
    date_dataframes = {
        date: group for date, group in combined_gdf.groupby(combined_gdf[date_col])
    }
    return date_dataframes


def write_to_dates_csv(dataframes, path):
    for date, date_gdf in dataframes.items():
        date_gdf.to_csv(f"{path}/csv/{date}.csv", index=False)
    print(dataframes)


if __name__ == "__main__":
    main()
