# Near real-time rainfall pipeline

Near real-time rainfall monitoring. Part of the project RIPOSTE for Cameroon Red Cross Society.

## Description
Data consumed:
- Level 3 IMERG Early Run: PPS Near Real-time, see https://gpm.nasa.gov/data/directory
- Value: 1-day precipitation accumulation (mm)
- Link to download: https://jsimpsonhttps.pps.eosdis.nasa.gov/imerg/gis/early/
- Temporal range: most recent past days (e.g. last 14 days)

The pipeline roughly consists of three steps:
- Extract the 1-day data on rainfall measurement as above.
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
3. Edit the configuration in `config\config.yaml`
   ```
   - name: <country-iso3>
     days-to-observe: <val> # number of most recent days to observe rainfall e.g. 14
     alert-on-threshold: <val> # threshold to send to EspoCRM e.g. 50
     shapefile-area: <name>.geojson # shapefile of areas (. geojson) where the zonal stats bases on
     espo-area:  # entity storing areas code (and id)
       entity: <entity-name>
       field: <id-field-name>
     espo-destination: # entity to send alerts to
       entity: <entity-name>
       field: <rainfall-field-name>
   ```
4. Run the pipeline : `python nrt_rainfall_pipeline.py --extract --transform --send`
    ```
    Usage: nrt_rainfall_pipeline.py [OPTIONS]

    Options:
    --country TEXT  country ISO3
    --extract       extract NRT rainfall raster data
    --transform     calculate rainfall data in pre-defined administrative areas
    --send          send to EspoCRM
    --dateend       specify a customed latest date YYYY-mm-dd until which the data should be extracted, by default it is the date before today
    --help          Show this message and exit
    ```

__Note:__ Payload sent to EspoCRM
```
    {
        "status": "onhold",
        "type": "heavyrainfall", 
        "source": "GPM",
        "<espo-area-field>": "<id-field-name>",
        "<espo-destination-field>": "<rainfall-field-name>"
    }
```
Where:
- `<espo-area-field>`: is the value of `espo-area`'s `field` in the config
- `<id-field-name>`: is to be automatically filled in the pipeline
- `<espo-destination-field>`: is the value of `espo-destination`'s `field` in the config
- `<rainfall-field-name>`: is to be automatically filled in the pipeline

## Adding new country
1. Prepare shapefile
- Add a shapefile in `.geojson` format of the area (e.g. districts) in `data\admin_boundary`
- Rename header of the area code (e.g. district code) to `code`
2. Prepare EspoCRM entity for area
- Create an entity to store the area (e.g. District)
- In the entity, create a field to store area code
- Import the area (by exporting the shapefile to `.csv`) to this entity
- Make sure the area code in this entity exactly the same to those in the shapefile
3. Prepare EspoCRM entity for alert
- Create an entity to store alert (e.g. Climate Hazard)
- In the entity, create a ` float` field to store calculated rainfall value
- In the entity, create additional fields to match with the payload requirements:
    | field name | type | value(s) | 
    | -----------| ---- | ---------|
    | `status`   | `enum` | `onhold` |
    | `type`    | `enum` | `heavyrainfall` |
    | `source`   | `enum` or `text` | `GPM` |
- Make sure the entity for area is linked with this one
4. Add the new country to the `config\config.yaml`, see Step 3 in __Basic usage__:
5. Test and adjust settings if needed.