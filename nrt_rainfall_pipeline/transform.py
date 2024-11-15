import rasterio
import pandas as pd
import datetime
import glob
import geopandas as gpd
from rasterstats import zonal_stats
from nrt_rainfall_pipeline.secrets_settings import Secrets
from nrt_rainfall_pipeline.settings import Settings
from nrt_rainfall_pipeline.load import Load


class Transform:
    
    def __init__(self, dateend, settings: Settings = None, secrets: Secrets = None):
        self.secrets = None
        self.settings = None
        self.loads = Load()
        if settings is not None:
            self.set_settings(settings)
        if secrets is not None:
            self.set_secrets(secrets)
        self.dateend = dateend
        self.datestart = dateend - datetime.timedelta(days=self.settings.get_setting("rainfall_threshold"))
    
    def set_settings(self, settings):
        """Set settings"""
        if not isinstance(settings, Settings):
            raise TypeError(f"invalid format of settings, use settings.Settings")
        settings.check_settings([])
        self.settings = settings

    def set_secrets(self, secrets):
        """Set secrets based on the data source"""
        if not isinstance(secrets, Secrets):
            raise TypeError(f"invalid format of secrets, use secrets.Secrets")
        secrets.check_secrets([])
        self.secrets = secrets

    def compute_average_rainfall(self):
        self.calculate_average_raster()
        stats = self.calculate_zonalstats()
        stats_list = self.prepare_data_for_espo(stats)
        checked = self.check_threshold(stats_list)
        return checked

    def calculate_average_raster(self):
        """
        Sum precipiatation per cell of all raster files and take average per cell
        """
        all_files = glob.glob('data/gpm/CRM_*.tif')
        n = len(all_files)

        with rasterio.open(all_files[0]) as src:
            result_array = src.read()
            result_profile = src.profile 

        for f in all_files[1:]:
            with rasterio.open(f) as src:
                result_profile = src.profile
                result_array = result_array + src.read()
        result_array = result_array/n

        file_name = f"CRM_{self.datestart.strftime('%Y-%m-%d')}_{self.dateend.strftime('%Y-%m-%d')}"
        with rasterio.open(f"data/gpm/{file_name}.tif", 'w', **result_profile) as dst:
            dst.write(result_array, indexes=[1])

    # transform
    def calculate_zonalstats(self):
        shp_name = "cmr_admbnda_adm1_inc_20180104"
        shp_dir = f"data/admin_boundary/{shp_name}.shp"
        shapefile = gpd.read_file(f"{shp_dir}")
        df_shp = pd.DataFrame(shapefile)
        tif_name = f"CRM_{self.datestart.strftime('%Y-%m-%d')}_{self.dateend.strftime('%Y-%m-%d')}"
        stats = zonal_stats(shapefile,
                            f"data/gpm/{tif_name}.tif",
                            stats=['median'],
                            geojson_out=True)
        return stats


    def prepare_data_for_espo(self, stats):
        """
        Prepare zonal stats data into payload matching EspoCRM requirements
        """
        additional_data = {"status": "onhold",
                           "type": "heavyrainfall", 
                           "source": "GPM"}
        admin1_pcode_id = self.loads.get_admin_id("CAdminLevel1", "admin1Pcode")
        stats_list = []
        for d in stats:
            new_d = {k: d["properties"][k] for k in ["ADM1_PCODE","median"]}
            new_d["cAdminLevel1Id"] = admin1_pcode_id.get(new_d["ADM1_PCODE"], new_d["ADM1_PCODE"])
            del new_d['ADM1_PCODE']
            new_d["average14dayRainfall"] = new_d.pop('median')
            new_d.update(additional_data)
            new_d["cAdminLevel1Id"] = admin1_pcode_id.get(new_d["cAdminLevel1Id"], new_d["cAdminLevel1Id"])
            # new_d["cHealthDistrictId"] = " "
            stats_list.append(new_d)
        return stats_list
    
    def check_threshold(self, stats_list):
        """
        Keep only records that has 
        """
        threshold = self.settings.get_setting("rainfall_threshold")
        checked = []
        for d in stats_list:
            if d["average14dayRainfall"] >= threshold:
                checked.append(d)
        return checked