import json
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pytest
import os
from dagster import DagsterInstance, build_asset_context
import pandas as pd

from downscaled_climate_data.assets.loca2 import (ESMCatalogConfig,
                                                  loca2_esm_catalog, parse_key)


@pytest.fixture
def s3(mocker):
    s3 = mocker.MagicMock()
    s3_client = mocker.MagicMock()
    s3.get_client.return_value = s3_client

    paginator = mocker.MagicMock()
    s3_client.get_paginator.return_value = paginator
    return s3


def set_bucket_contents(s3, keys):
    contents = [{"Key": key} for key in keys]

    paginator = s3.get_client.return_value.get_paginator.return_value
    paginator.paginate.return_value = [{"Contents": contents}]


@pytest.mark.parametrize(
    "key, expected_variable, expected_model, expected_scheme, expected_experiment_id, expected_time_range, expected_path",  # noqa: E501
    [
        ("ACCESS-CM2/historical/tasmin.ACCESS-CM2.historical.r3i1p1f1.1950-2014.LOCA_16thdeg_v20220413.monthly.cent.zarr",  # noqa: E501
            "tasmin", "ACCESS-CM2", "historical", "r3i1p1f1", "1950-2014",
         "s3://test-bucket/zarr/LOCA2/monthly/ACCESS-CM2/historical/tasmin.ACCESS-CM2.historical.r3i1p1f1.1950-2014.LOCA_16thdeg_v20220413.monthly.cent.zarr"),  # noqa: E501
        ("ACCESS-ESM1-5/historical/tasmin.ACCESS-ESM1-5.historical.r5i1p1f1.1950-2014.LOCA_16thdeg_v20220413.monthly.cent.zarr",  # noqa: E501
            "tasmin", "ACCESS-ESM1-5", "historical", "r5i1p1f1", "1950-2014",
         "s3://test-bucket/zarr/LOCA2/monthly/ACCESS-ESM1-5/historical/tasmin.ACCESS-ESM1-5.historical.r5i1p1f1.1950-2014.LOCA_16thdeg_v20220413.monthly.cent.zarr"),  # noqa: E501
    ]
)
def test_parse_key(key, expected_variable, expected_model, expected_scheme,
                   expected_experiment_id, expected_time_range, expected_path):
    parsed = parse_key(
        key,
        "test-bucket",
        "zarr/LOCA2/monthly/" + key
    )
    assert parsed == {
        "variable": expected_variable,
        "model": expected_model,
        "scheme": expected_scheme,
        "experiment_id": expected_experiment_id,
        "time_range": expected_time_range,
        "path": expected_path
    }


def test_generate_catalog_zarr(s3):
    instance = DagsterInstance.ephemeral()
    set_bucket_contents(s3, [
        "zarr/LOCA2/monthly/ACCESS-CM2/historical/tasmin.ACCESS-CM2.historical.r3i1p1f1.1950-2014.LOCA_16thdeg_v20220413.monthly.cent.zarr/time/0",  # noqa: E501
        "zarr/LOCA2/monthly/ACCESS-CM2/historical/tasmin.ACCESS-CM2.historical.r3i1p1f1.1950-2014.LOCA_16thdeg_v20220413.monthly.cent.zarr/lon/0",   # noqa: E501
        "zarr/LOCA2/monthly/MIROC6/ssp370/pr.MIROC6.ssp370.r2i1p1f1.2045-2074.LOCA_16thdeg_v20240915.cent.monthly.zarr/pr_tavg/3.0.1cent.zarr"      # noqa: E501
    ])

    os.environ['LOCA2_ZARR_PATH_ROOT'] = 'zarr/LOCA2/monthly'
    os.environ['LOCA2_BUCKET'] = 'test_bucket'
    with TemporaryDirectory() as tmpdir:
        with patch('downscaled_climate_data.assets.loca2.TemporaryDirectory') as mock_temp:  # noqa: E501
            mock_temp.return_value.__enter__.return_value = str(tmpdir)
            ctx = build_asset_context(instance=instance,
                                      resources={
                                          "s3": s3
                                      })

            config = ESMCatalogConfig(
                data_format="zarr",
                id="loca2_zarr_monthly_esm_catalog",
                description="LOCA2 zarr data catalog"
            )

            loca2_esm_catalog(ctx, config)

            with open(f"{tmpdir}/loca2_zarr_monthly_esm_catalog.json", "r") as json_file:
                collection_config = json.load(json_file)
                assert collection_config['id'] == "loca2_zarr_monthly_esm_catalog"

            with open(f"{tmpdir}/loca2_zarr_monthly_esm_catalog.csv", 'r') as csv_file:
                cat = pd.read_csv(csv_file)
                assert cat.shape == (2, 6)

                filtered_df = cat[cat["experiment_id"] == "r3i1p1f1"]
                r31_series = filtered_df.iloc[0]
                assert r31_series['variable'] == "tasmin"
                assert r31_series['model'] == "ACCESS-CM2"
                assert r31_series['scheme'] == "historical"
                assert r31_series['experiment_id'] == "r3i1p1f1"
                assert r31_series['time_range'] == "1950-2014"
                assert r31_series['path'] == "s3://test_bucket/zarr/LOCA2/monthly/ACCESS-CM2/historical/tasmin.ACCESS-CM2.historical.r3i1p1f1.1950-2014.LOCA_16thdeg_v20220413.monthly.cent.zarr"   # noqa: E501
                print(cat)


def test_generate_catalog_netcdf(s3):
    instance = DagsterInstance.ephemeral()
    set_bucket_contents(s3, [
        "netcdf/LOCA2/monthly/ACCESS-CM2/ssp585/tasmin.ACCESS-CM2.ssp585.r3i1p1f1.2075-2100.LOCA_16thdeg_v20220413.monthly.cent.nc",    # noqa: E501
        "netcdf/LOCA2/monthly/ACCESS-ESM1-5/historical/tasmin.ACCESS-ESM1-5.historical.r5i1p1f1.1950-2014.LOCA_16thdeg_v20220413.monthly.cent.nc"   # noqa: E501
    ])

    os.environ['LOCA2_RAW_PATH_ROOT'] = 'netcdf/LOCA2/monthly'
    os.environ['LOCA2_BUCKET'] = 'test_bucket'
    with TemporaryDirectory() as tmpdir:
        with patch('downscaled_climate_data.assets.loca2.TemporaryDirectory') as mock_temp:     # noqa: E501
            mock_temp.return_value.__enter__.return_value = str(tmpdir)
            ctx = build_asset_context(instance=instance,
                                      resources={
                                          "s3": s3
                                      })

            config = ESMCatalogConfig(
                data_format="netcdf",
                id="loca2_raw_monthly_esm_catalog",
                description="LOCA2 raw data catalog"
            )

            loca2_esm_catalog(ctx, config)

            with open(f"{tmpdir}/loca2_raw_monthly_esm_catalog.json", "r") as json_file:
                collection_config = json.load(json_file)
                assert collection_config['id'] == "loca2_raw_monthly_esm_catalog"

            with open(f"{tmpdir}/loca2_raw_monthly_esm_catalog.csv", 'r') as csv_file:
                cat = pd.read_csv(csv_file)
                assert cat.shape == (2, 6)

                filtered_df = cat[cat["experiment_id"] == "r3i1p1f1"]
                r31_series = filtered_df.iloc[0]
                assert r31_series['variable'] == "tasmin"
                assert r31_series['model'] == "ACCESS-CM2"
                assert r31_series['scheme'] == "ssp585"
                assert r31_series['experiment_id'] == "r3i1p1f1"
                assert r31_series['time_range'] == "2075-2100"
                assert r31_series['path'] == "s3://test_bucket/netcdf/LOCA2/monthly/ACCESS-CM2/ssp585/tasmin.ACCESS-CM2.ssp585.r3i1p1f1.2075-2100.LOCA_16thdeg_v20220413.monthly.cent.nc"   # noqa: E501
                print(cat)
