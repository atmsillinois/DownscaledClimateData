import os
from unittest.mock import patch
from dagster import DagsterInstance, build_asset_context

from downscaled_climate_data.assets.loca2 import loca2_zarr


@patch('downscaled_climate_data.assets.loca2.xr')
@patch('downscaled_climate_data.assets.loca2.s3fs')
def test_as_zarr_asset(mock_s3fs, mock_xarray, mocker):
    instance = DagsterInstance.ephemeral()

    s3 = mocker.MagicMock()
    s3.aws_access_key_id = "test_key"
    s3.aws_secret_access_key = "test_secret"
    s3.endpoint_url = "https://test"

    ctx = build_asset_context(instance=instance,
                              resources={
                                  "s3": s3
                              })

    mock_s3fs.S3FileSystem = mocker.MagicMock()
    mock_fs_open = mocker.MagicMock()
    mock_s3fs.S3FileSystem.return_value.open = mock_fs_open

    mock_fs_open.__enter__.return_value = mocker.MagicMock()

    mock_ds = mocker.MagicMock()
    mock_xarray.open_dataset.return_value = mock_ds

    os.environ['LOCA2_RAW_PATH_ROOT'] = 'test'
    os.environ['LOCA2_ZARR_PATH_ROOT'] = 'test/zarr'

    loca2_zarr(context=ctx, loca2_raw_netcdf={
        "bucket": "test_bucket",
        "s3_key": "/hist/cent.nc"
    })

    mock_s3fs.S3FileSystem.assert_called_with(
        key="test_key",
        secret="test_secret",
        endpoint_url="https://test"
    )

    mock_fs_open.assert_called_with("s3://test_bucket/test/hist/cent.nc", 'rb')

    mock_xarray.open_dataset.assert_called_with(
        mock_fs_open.return_value.__enter__.return_value
    )

    mock_s3fs.S3Map.assert_called_with(
        root="s3://test_bucket/test/zarr/hist/cent.zarr",
        s3=mock_s3fs.S3FileSystem.return_value,
        check=False
    )

    mock_ds.to_zarr.assert_called_with(
        store=mock_s3fs.S3Map.return_value,
        mode='w',
        consolidated=True
    )

    mock_ds.close.assert_called()
