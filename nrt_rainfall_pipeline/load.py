from __future__ import annotations
from nrt_rainfall_pipeline.secrets_settings import Secrets
from nrt_rainfall_pipeline.settings import Settings
from nrt_rainfall_pipeline.espo_api_client import EspoAPI
from nrt_rainfall_pipeline.logger import logger


class Load:

    def __init__(self, settings: Settings = None, secrets: Secrets = None):
        self.secrets = None
        self.settings = None
        if settings is not None:
            self.set_settings(settings)
        if secrets is not None:
            self.set_secrets(secrets)

    def set_settings(self, settings):
        """Set settings"""
        if not isinstance(settings, Settings):
            raise TypeError(f"invalid format of settings, use settings.Settings")
        self.settings = settings

    def set_secrets(self, secrets):
        """Set secrets for storage"""
        if not isinstance(secrets, Secrets):
            raise TypeError(f"invalid format of secrets, use secrets.Secrets")
        secrets.check_secrets(["ESPOCRM_URL", "ESPOCRM_API_KEY"])
        self.secrets = secrets

    def send_to_espo_api(self, country, data: list):
        logger.info("send data to EspoCRM")
        self.country = country
        destination = self.settings.get_country_setting(
            self.country, "espo-destination"
        )
        entity = destination["entity"]
        espo_client = EspoAPI(
            self.secrets.get_secret("ESPOCRM_URL"),
            self.secrets.get_secret("ESPOCRM_API_KEY"),
        )
        for data in data:
            espo_client.request("POST", entity, data)

    def get_admin_id(self, entity: str, pcode_col: str):
        """
        Get admin id in Espo based on Pcode field
        """
        espo_client = EspoAPI(
            self.secrets.get_secret("ESPOCRM_URL"),
            self.secrets.get_secret("ESPOCRM_API_KEY"),
        )
        admin1 = espo_client.request("GET", entity)
        admin1_filtered = self.__filter_dict(admin1["list"], [pcode_col, "id"])
        admin1_pcode_id = dict(item.values() for item in admin1_filtered)
        return admin1_pcode_id

    # transform
    def __filter_dict(self, dict: list, selected_keys: list):
        """
        Return list of dict with only selected keys
        """
        filtered = []
        for d in dict:
            d_filtered = {k: d[k] for k in selected_keys}
            filtered.append(d_filtered)
        return filtered
