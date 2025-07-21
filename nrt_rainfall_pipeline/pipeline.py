from nrt_rainfall_pipeline.extract import Extract
from nrt_rainfall_pipeline.load import Load
from nrt_rainfall_pipeline.transform import Transform
from nrt_rainfall_pipeline.secrets_settings import Secrets
from nrt_rainfall_pipeline.settings import Settings
from nrt_rainfall_pipeline.logger import logger
from datetime import datetime, timezone


class Pipeline:
    """Base class for flood data pipeline"""

    def __init__(self, settings: Settings, secrets: Secrets, country: str):
        self.settings = settings
        if country not in [c["name"] for c in self.settings.get_setting("countries")]:
            raise ValueError(f"No config found for country {country}")
        self.country = country
        self.load = Load(settings=settings, secrets=secrets)
        self.extract = Extract(settings=settings, secrets=secrets)
        self.transfrom = Transform(settings=settings, secrets=secrets)

    def run_pipeline(
        self,
        extract: bool = True,
        transform: bool = True,
        send: bool = True,
        save: bool = True,
        dateend: datetime = datetime.now(timezone.utc),
        # debug: bool = False
    ):
        """Run the rainfall data pipeline"""
        logger.info(f"Start rainfall pipeline at {datetime.now(timezone.utc)} UTC")

        if extract:  # download data
            self.extract.get_data(country=self.country, dateend=dateend)
            if save:
                pass

        average_rainfall = []
        if transform:
            average_rainfall = self.transfrom.compute_rainfall(
                country=self.country, dateend=dateend
            )
            if save:
                pass

        if send:  # send to espo
            self.load.send_to_espo_api(country=self.country, data=average_rainfall)
