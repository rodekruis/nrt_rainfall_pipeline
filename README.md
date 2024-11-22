# Near real-time rainfall pipeline

Near real-time rainfall monitoring. Part of the project RIPOSTE for Cameroon Red Cross Society.

## Description
The pipeline roughly consists of three steps:

- Extract daily data on rainfall measurement of the past days (e.g. last 14 days) from [NOAA's GPM](https://gpm.nasa.gov/data/directory)
- Transform the data into pre-defined areas (health districts) and calculate average rainfall of the past days. Then determine which area has its average rainfall higher than pre-defined thresholds.
- Send this data as alert to the EspoCRM for the NS.

## Basic Usage
To run the pipeline locally

1. Fill in the secrets in .env.example and rename the file to .env; in this way, they will be loaded as environment variables
2. Install requirements
    ```
    pip install poetry
    poetry install --no-interaction
    ```
3. Run the pipeline : `python nrt_rainfall_pipeline.py --extract --transform --send`
    ```
    Usage: nrt_rainfall_pipeline.py [OPTIONS]

    Options:
    --country TEXT  country ISO3
    --extract       extract rainfall data
    --transform     calculate rainfall data into pre-defined
    --send          send to IBF app
    --dateend       specify date until which the data should be extracted
    --help          Show this message and exit.
    ```
