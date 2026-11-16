import zipfile as z
from constants import *
from datetime import datetime
from zoneinfo import ZoneInfo


def get_shp_from_zip(zip_file_path):
    try:
        with z.ZipFile(zip_file_path, "r") as archive:
            names = archive.namelist()
            shapes = [name for name in names if name.lower().endswith(".shp")]
            if len(shapes) != 1:
                raise ValueError("NRSC archive must contain exactly one shapefile; missing data is not zero fires")
            shape = shapes[0]
            stem = shape.rsplit(".", 1)[0].lower()
            members = {name.lower() for name in names}
            if any(stem + ext not in members for ext in (".shx", ".dbf", ".prj")):
                raise ValueError("NRSC archive is missing shapefile components")
            bad_member = archive.testzip()
            if bad_member:
                raise ValueError("NRSC archive has a corrupt member")
            return shape
    except z.BadZipFile as exc:
        raise ValueError("NRSC download is not a valid ZIP archive") from exc


def get_todays_date():
    return datetime.now(tz=ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d")


def get_time_string():
    return datetime.now(tz=ZoneInfo("Asia/Kolkata")).strftime("%I:%M %p, %d %b %Y").lstrip("0")


def get_save_path(temp):
    if temp:
        return TMP_PATH
    else:
        return FILES_PATH
