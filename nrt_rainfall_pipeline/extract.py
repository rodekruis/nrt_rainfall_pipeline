import rasterio
from rasterio.mask import mask
from datetime import timedelta
import os
import subprocess
import time
import urllib
import geopandas as gpd
from zipfile import ZipFile
from nrt_rainfall_pipeline.secrets_settings import Secrets
from nrt_rainfall_pipeline.settings import Settings
from nrt_rainfall_pipeline.load import Load
from nrt_rainfall_pipeline.logger import logger


class Extract:
    """
    Extract near real-time observed rainfall data from external sources
    """

    def __init__(self, settings: Settings = None, secrets: Secrets = None):
        self.secrets = None
        self.settings = None
        self.load = Load()
        self.inputGPM = "./data/gpm"
        if not os.path.exists(self.inputGPM):
            os.makedirs(self.inputGPM)
        if settings is not None:
            self.set_settings(settings)
            self.load.set_settings(settings)
        if secrets is not None:
            self.set_secrets(secrets)
            self.load.set_secrets(secrets)

    def set_settings(self, settings):
        """Set settings"""
        if not isinstance(settings, Settings):
            raise TypeError(f"invalid format of settings, use settings.Settings")
        settings.check_settings(["days-to-observe", "alert-on-threshold"])
        self.settings = settings

    def set_secrets(self, secrets):
        """Set secrets based on the data source"""
        if not isinstance(secrets, Secrets):
            raise TypeError(f"invalid format of secrets, use secrets.Secrets")
        secrets.check_secrets(["EOSDIS_URL", "EOSDIS_USERNAME", "EOSDIS_PASSWORD"])
        self.secrets = secrets

    def get_data(self, country: str, dateend):
        """
        Get observed rainfall data from source and
        """
        self.country = country
        days_to_observe = self.settings.get_country_setting(
            self.country, "days-to-observe"
        )
        logger.info(
            f"Get rainfall data from {dateend - timedelta(days=days_to_observe)} to {dateend}"
        )
        for n in range(0, days_to_observe):
            filedate = dateend - timedelta(days=n)
            file_name, file_url = self.__define_file_url(filedate)
            is_file_available = self.__download_rainfall(
                self.secrets.get_secret("EOSDIS_USERNAME"),
                self.secrets.get_secret("EOSDIS_PASSWORD"),
                file_name,
                file_url,
            )
            if is_file_available:
                self.__prepare_rainfall_data(file_name)
            else:
                logger.warning(f"{file_url} not available!")

    def __define_file_url(self, filedate):
        """
        Get filedate and return file name, file url
        """
        base_url = self.secrets.get_secret("EOSDIS_URL")
        filedate_year = filedate.year
        filedate_month = filedate.month
        filedate_day = filedate.day
        file_name = f"""3B-DAY-L.GIS.IMERG.{filedate_year}{filedate_month:02d}{filedate_day:02d}.V07B"""
        file_url = (
            f"""{base_url}/{filedate_year}/{filedate_month:02d}/{file_name}.zip"""
        )
        return file_name, file_url

    def __download_rainfall(self, username, password, file_name, file_url) -> bool:
        """
        Donwnload the rainfall data zip file and extract the zip file.
        Retry 5 times max if failed
        """
        no_attempts, attempt, is_file_available = 5, 0, True
        while attempt < no_attempts:
            try:
                is_file_available = self.__get_rainfall(
                    username, password, file_name, file_url
                )
                time.sleep(10)
                break
            except urllib.error.URLError:
                attempt += 1
                time.sleep(120)
        if attempt == no_attempts:
            raise ConnectionError("GPM server not available")
        return is_file_available

    def __get_rainfall(self, username, password, file_name, file_url) -> bool:
        if not os.path.isfile(f"{self.inputGPM}/{file_name}.zip"):
            download_command = f"""wget -nv -P {self.inputGPM} --user={username} --password={password} {file_url}"""
            try:
                subprocess.call(
                    download_command,
                    cwd=".",
                    shell=True,
                )
            except FileNotFoundError:
                pass
        if os.path.exists(f"{self.inputGPM}/{file_name}.zip"):
            with ZipFile(f"{self.inputGPM}/{file_name}.zip", "r") as zf:
                zf.extract(f"{file_name}.tif", path=f"{self.inputGPM}")
        return os.path.exists(f"{self.inputGPM}/{file_name}.tif")

    def __prepare_rainfall_data(self, file_name):
        """
        For each date (file), slice it to the extent of the country
        """
        shp_name = self.settings.get_country_setting(self.country, "shapefile-area")
        shp_dir = f"data/admin_boundary/{shp_name}"
        shapefile = gpd.read_file(f"{shp_dir}")
        shapes = [feature["geometry"] for feature in shapefile.iterfeatures()]
        with rasterio.open(f"{self.inputGPM}/{file_name}.tif") as src:
            out_image, out_transform = mask(src, shapes, crop=True)
            out_meta = src.meta
        out_meta.update(
            {
                "driver": "GTiff",
                "height": out_image.shape[1],
                "width": out_image.shape[2],
                "transform": out_transform,
            }
        )
        with rasterio.open(
            f"{self.inputGPM}/{self.country}_{file_name}.tif", "w", **out_meta
        ) as dest:
            dest.write(out_image)
