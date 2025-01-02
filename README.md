# DownscaledClimateData
Data Pipelines for Downscaled Climate Datasets

Dagster port of original work by Maile Sasaki in the [Climate_Map Repo](https://github.com/mailesasaki/climate_map).

This is a dagster project that downloads and processes data from various downscaled climate datasets. 
The data is stored in a cloud bucket and is for use in the University of Illinois 
Department of Climate, Meteorology & Atmospheric Sciences analysis facility. 

The goal of this project is to deploy a set of sensors and jobs to maintain data assets of downscaled
climate data. The datasets will include:
- Raw netcdf files downloaded from the sources
- Cloud optimized Zarr files
- Analyzed results ready for display on the Climate Map

## Getting Started
Dagster offers a great developer experience. It's easy to run a dagster engine on your laptop and 
try out these pipelines.

### Install the Project
Create a virtual environment and install the project and the development dependencies.

```bash
python3 -m venv .venv

source .venv/bin/activate
pip install -e ".[dev]"
```

### Configure Access to Storage Bucket
The goal of the project is to store data in a cloud bucket. You can experiment with some
portions of the pipeline without a cloud bucket, but you will need to configure the bucket
to run the full pipeline.

Access token and URL are read through environment variables. Dagster offers an easy way to 
configure these through a `.env` file.

Create this file in the root of the project and add the following lines:

```bash
S3_ENDPOINT_URL=https://url-of-your-s3-provider
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-access-key

LOCA2_BUCKET=loca2-data

# No leading slashes on these paths - the downloaded netcdf and zarr files will
# be stored in subdirectories of these paths
LOCA2_ZARR_PATH_ROOT=zarr/LOCA2
LOCA2_RAW_PATH_ROOT=raw/LOCA2
```

The .env file is already in the `.gitignore` file so you don't have to worry about accidentally
committing your secrets.

### Run the Dagster Dashboard
The [Dagster UI](https://docs.dagster.io/concepts/webserver/ui) is a web-based interface 
for viewing and interacting with Dagster objects. Since we have already installed this project 
in the virtual environment, you can just launch the dagster UI with the command:

```bash 
dagster dev
```

This will run the web server and print out a local url for you to visit that will get you into 
the dashboard.

### Development Configuration
The project includes a `dagster.yaml` file that sets up the configuration for the project.
It currently has a configuration to limit the number of concurrent workers to 1 to avoid
blowing up your laptop.

## Dagster Project Structure
There are three main concepts in the Dagster project:
1. Assets - The data that is produced by the pipelines. They are implemented by python code. Assets can depend on other assets
2. Sensors - The sensors are used to monitor sources for new data. They can run on a schedule and trigger asset jobs.
3. Resources - The resources are the connections to external systems. They are used to connect to the cloud storage and other services.

Here are descriptions of the assets, sensors, and resources that make up the project.

### Assets
[loca2_raw_netcdf](downscaled_climate_data/assets/loca2.py)

This asset represents the raw netcdf data downloaded from the LOCA2 dataset. 
The data is stored in a cloud bucket and can be used as the source for the other assets. It accepts 
the following parameters:
- `url` - The url of the netcdf file from the UCSD web server
- `bucket` - The name of the cloud bucket where the data will be stored
- `s3_key` - The key of the object in the bucket. This is the full path where the object will be stored. It looks like a directory structure.

These values are typically produced by the `Loca2Datasets` resource.

[loca2_zarr](downscaled_climate_data/assets/loca2.py
Convert the netcdf files to Zarr format. This asset uses the `xarray` library to read the netcdf file and
convert it to Zarr format. The Zarr format is a cloud optimized format that is more efficient for reading
data in the cloud. The asset accepts the output from the `loca2_raw_netcdf` asset as input.


### Resources
These resources are consumed by the sensor to make the entire pipeline easily configurable and to
encapsulate the interactions with the UCSD web server and the cloud storage.

`Loca2Models` - This resource doesn't actually interact with the outside world, but is the source of the 
dictionary of models and scenarios that are used in the LOCA2 dataset. This resource is used to drive
the search for new files on the UCSD web server.

`Loca2Datasets` - This resource is used to interact with the UCSD web server to find new files in the LOCA2 
dataset for a specific model/scenario combination. It uses the `beautifulsoup4` library to parse the
web pages and find the links to the netcdf files. It filters out any monthly summary files. This resource works 
like an interator and yields one record per file on the webserver. The record is a dictionary with the
following keys:
- `model` - The name of the model
- `scenario` - The name of the scenario
- `member ID` - The member ID of the model
- `variable` - The variable represented in the dataset
- `url` - The url of the netcdf file
- `s3_key` - The suggested S3 key this file should be saved as in the cloud bucket



### Sensors
[LOCA2_Sensor](downscaled_climate_data/sensors/loca2_sensor.py)

This sensor finds new data in the LOCA2 dataset and triggers the asset job to download the data. It 
creates an asset key to make sure we don't download the same file twice. Dagster, by design, wants
Sensors to complete in a short amount of time. We work at the model/scenario/member level to keep the
the number of files relatively small, so we can complete in time.

## Development
Since the instructions above specify installing the project with the `-e` flag, changes
to the source code are immediately in the Dagster UI. You do need to tell Dagster to reload
the project. This can be done by clicking the "Reload" button in the Dagster UI on the _Deployment_ tab.

The assets and sensors have unit tests which also simplify development since you can 
observe the code running in a variety of scenarios without the whole Dagster infrastructure. 
You can run  the tests with the command:

```bash
pytest
``` 