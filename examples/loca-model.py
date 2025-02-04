import os

import intake
from dotenv import load_dotenv

# Make sure your .env file is in the same directory as this script and
# contains the following line:
# S3_ENDPOINT_URL=https://rice1.osn.mghpcc.org
load_dotenv()

url = "s3://ees240146/loca2_zarr_monthly_esm_catalog.json"
catalog = intake.open_esm_datastore(url,
                                    storage_options={
                                        "anon": True,
                                        "endpoint_url": os.environ['S3_ENDPOINT_URL']
                                    })

catalog_subset = catalog.search(variable="tasmax", model="ACCESS-CM2", scheme="historical")
dsets = catalog_subset.to_dataset_dict(
    xarray_open_kwargs={"use_cftime": True, "engine": 'zarr'},
    storage_options={"anon": True, "endpoint_url": os.environ['S3_ENDPOINT_URL']}
)

print(dsets)
