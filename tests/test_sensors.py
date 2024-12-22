import os

import pytest

from dagster import DagsterInstance, build_sensor_context
from downscaled_climate_data.sensors.loca2_sensor import Loca2Datasets, loca2_sensor


@pytest.fixture
def models(mocker):
    mocked_models = mocker.Mock()

    # Intentionally not sorted to test the sorting in the sensor
    mocked_models.models = {
        "ACCESS-ESM1-5": {
            "historical": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1"},
            "ssp245": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1"},
            "ssp585": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1"}},
        "ACCESS-CM2": {"ssp245": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1"},
                       "historical": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1"},
                       "ssp370": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1"},
                       "ssp585": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1"}}
    }
    return mocked_models


@pytest.fixture
def downloadable_files(mocker):
    mocked_data = mocker.Mock()
    mocked_data.get_downloadable_files.return_value = [
        {"url": "https/foo/bar", "s3_key": "foo/bar",
         "model": "ACCESS-CM2", "scenario": "historical", "memberid": "r1i1p1f1"},
        {"url": "https/foo/bar2", "s3_key": "foo/bar2",
         "model": "ACCESS-CM2", "scenario": "historical", "memberid": "r4i1p1f1"}
    ]
    return mocked_data


@pytest.fixture
def bucket(mocker):
    mocked_bucket = mocker.Mock()
    mocked_bucket.bucket = "loca2_bucket"
    return mocked_bucket


def test_sensor(models, downloadable_files, bucket):
    instance = DagsterInstance.ephemeral()
    os.environ["LOCA2_BUCKET"] = "loca2_bucket"
    os.environ["LOCA2_PATH_ROOT"] = "/netcdf/LOCA2/"
    ctx = build_sensor_context(instance=instance,
                               resources={
                                   "loca2_models": models,
                                   "loca2_datasets": downloadable_files,
                                   "loca2_dataset_destination": bucket
                               }, cursor=None)
    data = loca2_sensor.evaluate_tick(ctx)

    run_requests = data.run_requests
    assert len(run_requests) == 2
    run_request = run_requests[0]
    # Assert overall RunRequest attributes
    assert run_request.run_key == 'foo/bar'

    # Validate run_config structure
    assert 'ops' in run_request.run_config
    assert 'RawLOCA2' in run_request.run_config['ops']

    # Check nested configuration details
    config = run_request.run_config['ops']['RawLOCA2']['config']
    assert config['url'] == 'https/foo/bar'
    assert config['bucket'] == 'loca2_bucket'
    assert config['s3_key'] == '/netcdf/LOCA2/foo/bar'

    # Validate tags
    assert run_request.tags == {
        'model': 'ACCESS-CM2',
        'scenario': 'historical',
        'memberid': 'r1i1p1f1',
        'dagster/sensor_name': 'LOCA2_Sensor'
    }

    assert data.cursor == "ACCESS-CM2/historical"


def test_sensor_existing_cursor(models, downloadable_files, bucket):
    instance = DagsterInstance.ephemeral()

    ctx = build_sensor_context(instance=instance,
                               resources={
                                   "loca2_models": models,
                                   "loca2_datasets": downloadable_files,
                                   "loca2_dataset_destination": bucket
                               }, cursor="ACCESS-CM2/historical")
    data = loca2_sensor.evaluate_tick(ctx)
    run_requests = data.run_requests
    assert len(run_requests) == 2
    run_request = run_requests[0]

    # Validate tags
    assert run_request.tags == {
        'model': 'ACCESS-CM2',
        'scenario': 'ssp245',
        'memberid': 'r1i1p1f1',
        'dagster/sensor_name': 'LOCA2_Sensor'
    }

    assert data.cursor == "ACCESS-CM2/ssp245"


def test_sensor_no_more_cursors(models, downloadable_files, bucket):
    instance = DagsterInstance.ephemeral()

    ctx = build_sensor_context(instance=instance,
                               resources={
                                   "loca2_models": models,
                                   "loca2_datasets": downloadable_files,
                                   "loca2_dataset_destination": bucket
                               }, cursor="ACCESS-ESM1-5/ssp585")
    data = loca2_sensor.evaluate_tick(ctx)
    run_requests = data.run_requests
    assert len(run_requests) == 0
    assert data.cursor == "ACCESS-ESM1-5/ssp585"


def test_loca2_dataset(mocker, models):
    resource = Loca2Datasets(variable='pr')
    files = list(
        resource.get_downloadable_files(
            models.models, 'ACCESS-CM2', 'historical')
    )
    assert len(files) == 3

    # Find the file with the expected memberid
    file_metadata = [entry for entry in files if entry['memberid'] == 'r3i1p1f1'][0]

    # Assert all expected keys are present
    expected_keys = ['model', 'scenario', 'memberid', 'variable', 'url', 's3_key']
    assert set(expected_keys).issubset(file_metadata.keys())

    # Validate specific values
    assert file_metadata['model'] == 'ACCESS-CM2'
    assert file_metadata['scenario'] == 'historical'
    assert file_metadata['memberid'] == 'r3i1p1f1'
    assert file_metadata['variable'] == 'pr'

    # Optional: Additional checks for URL components
    assert 'LOCA_16thdeg_v20240915' in file_metadata['url']
    assert file_metadata['url'].startswith('https://cirrus.ucsd.edu')
    assert file_metadata['url'].endswith('.nc')

    # S3 key validation
    assert file_metadata['s3_key'] == '/ACCESS-CM2/historical/pr.ACCESS-CM2.historical.r3i1p1f1.1950-2014.LOCA_16thdeg_v20240915.cent.nc'  # NOQA E501
