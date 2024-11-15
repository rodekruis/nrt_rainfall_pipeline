import rasterio
from rasterio.mask import mask
import datetime
import os
import subprocess
import time
import urllib
import geopandas as gpd
from zipfile import ZipFile
from nrt_rainfall_pipeline.secrets_settings import Secrets
from nrt_rainfall_pipeline.settings import Settings
from nrt_rainfall_pipeline.load import Load



class Extract:
    """
    Extract near real-time observed rainfall data from external sources
    """

    def __init__(
        self,
        dateend,
        settings: Settings = None,
        secrets: Secrets = None,
    ):
        self.secrets = None
        self.settings = None
        self.inputGPM = "./data/gpm"
        self.load = Load()
        if not os.path.exists(self.inputGPM):
            os.makedirs(self.inputGPM)
        if settings is not None:
            self.set_settings(settings)
            self.load.set_settings(settings)
        if secrets is not None:
            self.set_secrets(secrets)
            self.load.set_secrets(secrets)
        self.dateend = dateend

    def set_settings(self, settings):
        """Set settings"""
        if not isinstance(settings, Settings):
            raise TypeError(f"invalid format of settings, use settings.Settings")
        settings.check_settings(["no_days_monitor", "rainfall_threshold"])
        self.settings = settings

    def set_secrets(self, secrets):
        """Set secrets based on the data source"""
        if not isinstance(secrets, Secrets):
            raise TypeError(f"invalid format of secrets, use secrets.Secrets")
        secrets.check_secrets(["EOSDIS_URL", "EOSDIS_USERNAME", "EOSDIS_PASSWORD"])
        self.secrets = secrets


    def get_data(self, dateend):
        """
        Get observed rainfall data from source and 
        """
        # extract
        number_of_date = self.settings.get_setting("no_days_to_monitor")
        for n in range(0, number_of_date+1):
            filedate = dateend - datetime.timedelta(days=n)
            file_name, file_url = self.define_file_url(filedate)
            self.download_rainfall(self.secrets.get_secret("EOSDIS_USERNAME"), 
                                   self.secrets.get_secret("EOSDIS_PASSWORD"),
                                   file_name, file_url)
            self.prepare_rainfall_data(file_name)


    def define_file_url(self, filedate):
        '''
        Get filedate and return file name, file url
        '''
        base_url = self.secrets.get_secret("EOSDIS_URL")
        filedate_year = filedate.year
        filedate_month = filedate.month
        filedate_day = filedate.day
        file_name = f"""3B-DAY-L.GIS.IMERG.{filedate_year}{filedate_month:02d}{filedate_day:02d}.V07B"""
        file_url = f"""{base_url}/{filedate_year}/{filedate_month:02d}/{file_name}.zip"""
        return file_name, file_url


    def download_rainfall(self, username, password, file_name, file_url):
        '''
        Donwnload the rainfall data zip file and extract the zip file.
        Retry 5 times max if failed
        '''
        no_attempts, attempt, download_done = 5, 0, False
        while attempt < no_attempts:
            try:
                self.get_rainfall(username, password, file_name, file_url)
                download_done = True
                time.sleep(10)
                break
            except urllib.error.URLError:
                attempt += 1
                time.sleep(120)
        if not download_done:
            raise ConnectionError("GPM server not available")


    def get_rainfall(self, username, password, file_name, file_url):
        if not os.path.isfile(f"data/gpm/{file_name}.zip"):
            download_command = f"""wget -q --user={username} --password={password} {file_url} -P data/gpm"""
            subprocess.call(download_command, cwd=".", shell=True)
        with ZipFile(f"data/gpm/{file_name}.zip", 'r') as zf:
            zf.extract(f"{file_name}.tif", path="data/gpm") 


    def prepare_rainfall_data(self, file_name):
        '''
        For each date (file), slice it to the extent of the country
        '''
        shapefile_dir = 'data/admin_boundary/cmr_admbnda_adm0_inc_20180104.shp'
        shapefile = gpd.read_file(f"{shapefile_dir}")
        shapes = [feature["geometry"] for feature in shapefile.iterfeatures()]
        with rasterio.open(f"data/gpm/{file_name}.tif") as src:
            out_image, out_transform = mask(src, shapes, crop=True)
            out_meta = src.meta 
        out_meta.update({"driver": "GTiff",
                        "height": out_image.shape[1],
                        "width": out_image.shape[2],
                        "transform": out_transform})
        with rasterio.open(f"data/gpm/CRM_{file_name}.tif", "w", **out_meta) as dest:
            dest.write(out_image)

