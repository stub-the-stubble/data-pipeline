import zipfile as z
from datetime import datetime
from zoneinfo import ZoneInfo


def get_shp_from_zip(zip_file_path):
    with z.ZipFile(zip_file_path, "r") as zip_file:
        for name in zip_file.namelist():
            if name.endswith(".shp"):
                return name


def get_todays_date():
    return datetime.now(tz=ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d")


def get_time_string():
    return datetime.now(tz=ZoneInfo("Asia/Kolkata")).strftime("%-I:%M %p, %d %b %Y")
