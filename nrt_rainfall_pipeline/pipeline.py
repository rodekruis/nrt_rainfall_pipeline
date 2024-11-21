from nrt_rainfall_pipeline.extract import Extract
from nrt_rainfall_pipeline.load import Load
from nrt_rainfall_pipeline.transform import Transform
from nrt_rainfall_pipeline.secrets_settings import Secrets
from nrt_rainfall_pipeline.settings import Settings
from datetime import datetime, timezone, timedelta
import logging

logger = logging.getLogger()
logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("requests_oauthlib").setLevel(logging.WARNING)


class Pipeline:
    """Base class for flood data pipeline"""

    def __init__(self, settings: Settings, secrets: Secrets, country: str):
        self.settings = settings
        if country not in [c["name"] for c in self.settings.get_setting("countries")]:
            raise ValueError(f"No config found for country {country}")
        self.country = country
        self.load = Load(settings=settings, 
                         secrets=secrets)
        self.extract = Extract(
            settings=settings,
            secrets=secrets
        )
        self.transfrom = Transform(
            settings=settings,
            secrets=secrets
        )

    def run_pipeline(
        self,
        extract: bool = True,
        transform: bool = True,
        send: bool = True,
        save: bool =True,
        dateend: datetime = datetime.now(timezone.utc)-timedelta(days=1),
        # debug: bool = False
    ):
        """Run the flood data pipeline"""

        if extract: # download data
            logging.info(f"extract rainfall data")
            self.extract.get_data(country=self.country, dateend=dateend)
            if save:
                pass

        if transform:
            logging.info("compute average past days")
            average_rainfall = self.transfrom.compute_rainfall(country=self.country, dateend=dateend)
            if save:
                pass

        if send: # send to espo
            logging.info("send data to EspoCRM")
            self.load.send_to_espo_api(country=self.country, data=average_rainfall)