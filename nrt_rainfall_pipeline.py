from nrt_rainfall_pipeline.pipeline import Pipeline
from nrt_rainfall_pipeline.secrets_settings import Secrets
from nrt_rainfall_pipeline.settings import Settings
from datetime import timezone, datetime, timedelta
import click


@click.command()
@click.option("--country", help="country ISO3", default="CMR")
@click.option("--extract", help="extract NRT rainfall raster data", default=False, is_flag=True)
@click.option("--transform", help="calculate rainfall per admin", default=False, is_flag=True)
@click.option("--send", help="send to EspoCRM", default=False, is_flag=True)
@click.option("--save", help="save to storage", default=False, is_flag=True)
@click.option(
    "--datestart",
    help="date start in YYYY-mm-dd",
    default=(datetime.now(timezone.utc)-timedelta(days=1)).strftime("%Y-%m-%d"),
)
# @click.option(
#     "--debug",
#     help="debug mode: process only one ensemble member from yesterday",
#     default=False,
#     is_flag=True,
# )

def run_nrt_rainfall_pipeline(
    extract, transform, send, save, datestart#, debug
):
    datestart = datetime.strptime(datestart, "%Y-%m-%d")
    pipe = Pipeline(
        settings=Settings("config/config.yaml"),
        secrets=Secrets(".env"),
    )
    pipe.run_pipeline(
        extract=extract,
        transform=transform,
        send=send,
        save=save,
        # debug=debug,
        datestart=datestart
    )


if __name__ == "__main__":
    run_nrt_rainfall_pipeline()