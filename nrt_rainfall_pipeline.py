from nrt_rainfall_pipeline.pipeline import Pipeline
from nrt_rainfall_pipeline.secrets_settings import Secrets
from nrt_rainfall_pipeline.settings import Settings
from datetime import timezone, datetime, timedelta
import click


@click.command()
@click.option("--country", help="country ISO3", default="CMR")
@click.option("--extract", help="extract NRT rainfall raster data", default=False, is_flag=True)
@click.option("--transform", help="calculate rainfall data in pre-defined administrative areas", default=False, is_flag=True)
@click.option("--send", help="specify a customed latest date YYYY-mm-dd until which the data should be extracted, by default it is the date before today", default=False, is_flag=True)
@click.option("--save", help="save to storage", default=False, is_flag=True)
@click.option(
    "--dateend",
    help="date end in YYYY-mm-dd",
    default=(datetime.now(timezone.utc)-timedelta(days=2)).strftime("%Y-%m-%d"),
)

def run_nrt_rainfall_pipeline(
    country, extract, transform, send, save, dateend
):
    dateend = datetime.strptime(dateend, "%Y-%m-%d")
    pipe = Pipeline(
        country=country,
        settings=Settings("config/config.yaml"),
        secrets=Secrets(".env"),
    )
    pipe.run_pipeline(
        extract=extract,
        transform=transform,
        send=send,
        save=save,
        dateend=dateend
    )


if __name__ == "__main__":
    run_nrt_rainfall_pipeline()