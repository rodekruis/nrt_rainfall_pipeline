import rasterio
import pandas as pd
from datetime import timedelta
import glob
import geopandas as gpd
from rasterstats import zonal_stats
from nrt_rainfall_pipeline.secrets_settings import Secrets
from nrt_rainfall_pipeline.settings import Settings
from nrt_rainfall_pipeline.load import Load
from nrt_rainfall_pipeline.logger import logger


class Transform:

    def __init__(self, settings: Settings = None, secrets: Secrets = None):
        self.secrets = None
        self.settings = None
        self.load = Load()
        self.inputGPM = "./data/gpm"
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
        secrets.check_secrets(["ESPOCRM_URL", "ESPOCRM_API_KEY"])
        self.secrets = secrets

    def compute_rainfall(self, country: str, dateend):
        logger.info("Compute average rainfall among available raster files")
        self.country = country
        self.dateend = dateend
        days = self.settings.get_country_setting(self.country, "days-to-observe")
        self.datestart = dateend - timedelta(days=int(days) - 1)
        self.__calculate_average_raster()
        stats = self.__calculate_zonalstats()
        data_out = self.__prepare_data_for_espo(stats)
        return data_out

    def __calculate_average_raster(self):
        """
        Sum precipitation per cell of all raster files and take average per cell
        Scale precipitation x0.1
        """
        all_files = glob.glob(
            f"{self.inputGPM}/{self.country}_3B-DAY-L.GIS.IMERG.*.tif"
        )
        n = len(all_files)

        with rasterio.open(all_files[0]) as src:
            result_array = src.read()
            result_profile = src.profile

        for f in all_files:
            with rasterio.open(f) as src:
                result_profile = src.profile
                result_array = result_array + src.read()
        result_array = result_array * 0.1 / n

        file_name = f"{self.country}_{self.datestart.strftime('%Y-%m-%d')}_{self.dateend.strftime('%Y-%m-%d')}"
        with rasterio.open(
            f"{self.inputGPM}/{file_name}.tif", "w", **result_profile
        ) as dst:
            dst.write(result_array, indexes=[1])

    # transform
    def __calculate_zonalstats(self):
        shp_name = self.settings.get_country_setting(self.country, "shapefile-area")
        shp_dir = f"data/admin_boundary/{shp_name}"
        shapefile = gpd.read_file(f"{shp_dir}")
        tif_name = f"{self.country}_{self.datestart.strftime('%Y-%m-%d')}_{self.dateend.strftime('%Y-%m-%d')}"
        stats = zonal_stats(
            shapefile,
            f"{self.inputGPM}/{tif_name}.tif",
            stats=["median"],
            all_touched=True,
            geojson_out=True,
        )
        return stats

    def __prepare_data_for_espo(self, stats):
        """
        Prepare zonal stats data into payload matching EspoCRM requirements
        """
        area = self.settings.get_country_setting(self.country, "espo-area")
        area_entity = area["entity"]
        area_field = area["field"]
        destination = self.settings.get_country_setting(
            self.country, "espo-destination"
        )
        destination_field = destination["field"]
        additional_data = {"status": "onhold", "type": "heavyrainfall", "source": "GPM"}
        admin_id = self.load.get_admin_id(area_entity, "code")
        admin_id = self.__extract_id_from_key(admin_id)
        stats_list = []
        for d in stats:
            new_d = {k: d["properties"][k] for k in ["code", "median"]}
            new_d[area_field] = admin_id.get(new_d["code"], new_d["code"])
            del new_d["code"]
            new_d[destination_field] = new_d.pop("median")
            new_d.update(additional_data)
            stats_list.append(new_d)

        threshold = self.settings.get_country_setting(
            self.country, "alert-on-threshold"
        )
        filtered = self.__filter_dict(stats_list, destination_field, threshold)
        return filtered

    def __filter_dict(self, stats_list, key_to_filter: str, threshold: float):
        """
        Keep only records that has rainfall value exceeds the thresholds
        """
        checked = []
        for d in stats_list:
            rainfall_value = d[key_to_filter]
            if rainfall_value:
                if rainfall_value >= threshold:
                    checked.append(d)
        return checked

    def __extract_id_from_key(self, dict):
        """
        Extract id from key string
        """
        return {key[-3:]: value for key, value in dict.items()}
