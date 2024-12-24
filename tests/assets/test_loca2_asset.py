import os
from unittest.mock import patch

from dagster import DagsterInstance, build_asset_context

from downscaled_climate_data.assets.loca2 import Loca2Config, loca2_raw


def test_loca2_raw(mocker):
    instance = DagsterInstance.ephemeral()
    s3 = mocker.MagicMock()

    config = Loca2Config(
        url="https://my-test-url.com/test/cent.nc",
        s3_key="/loca2/cent.nc")

    ctx = build_asset_context(instance=instance,
                              resources={
                                  "s3": s3
                              })

    with patch('downscaled_climate_data.assets.loca2.requests.get') as mock_get:
        os.environ['LOCA2_BUCKET'] = 'test_bucket'
        os.environ['LOCA2_PATH_ROOT'] = '/test'
        mock_response = mocker.Mock()
        mock_response.headers = {'content-length': str(1024 ** 3)}
        mock_response.raw = "RawBytes"
        mock_get.return_value.__enter__.return_value = mock_response

        results = loca2_raw(context=ctx, config=config)

        assert results == {
            "bucket": "test_bucket",
            "s3_key": "/loca2/cent.nc"
        }

        mock_get.assert_called_with(config.url, stream=True)

        s3.get_client.return_value.upload_fileobj. \
            assert_called_with("RawBytes", "test_bucket", "test/loca2/cent.nc")
