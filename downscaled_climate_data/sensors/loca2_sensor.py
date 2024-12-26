import re
import urllib.error
import urllib.request
from typing import Iterable

from bs4 import BeautifulSoup
from dagster import (
    sensor,
    ConfigurableResource,
    SensorEvaluationContext,
    RunRequest,
    RunConfig,
)

from downscaled_climate_data.assets.as_zarr import as_zarr
from downscaled_climate_data.assets.loca2 import loca2_raw
from downscaled_climate_data.sensors.loca2_models import Loca2Models

# Give ourselves 2 hours to process a single model/scenario
LOCA2_SENSOR_FREQUENCY = 3600 * 2

# For the smaller, monthly files, we can process them more frequently
LOCA2_MONTHLY_SENSOR_FREQUENCY = 600

LOCA2_ASSETS = [loca2_raw, as_zarr]


class Loca2Datasets(ConfigurableResource):
    """
    Dagster resource that provides an iterator over LOCA2 files representing a variable
    for a given model and scenario.
    """
    variable: str = "tasmax"

    def get_downloadable_files(self, models: dict,
                               model: str, scenario: str,
                               monthly: bool):
        for memberid in models[model][scenario]:
            # Putting together the URL of the data location
            path_string = (
                "https://cirrus.ucsd.edu/~pierce/LOCA2/CONUS_regions_split/"
                + model + "/cent/0p0625deg/" + memberid + "/"
                + scenario + "/" + self.variable + "/"
            )

            try:
                path_soup = BeautifulSoup(
                    urllib.request.urlopen(path_string), "html.parser"
                )  # Parsing the website to look for the download
            except urllib.error.HTTPError as e:
                print("Can't find the path", path_string)
                raise e
            file_list = []
            for file in path_soup.find_all("a"):  # Pulling the links
                file_list.append(file.get("href"))

            # Create a regex to find just the data files. Sadly, the monthly files in the
            # pr variable have a different naming convention
            file_regex = fr"{self.variable}\.{model}\.{scenario}\.{memberid}\..*.LOCA_16thdeg_v\d+" # noqa E501
            file_regex += r"\.(monthly\.cent\.nc|cent\.monthly\.nc)" if monthly else r"\.cent\.nc" # noqa E501

            filtered = [f for f in file_list if re.match(file_regex, f)]

            directory = (
                "/" + model + "/" + scenario + "/"
            )  # Pulling out the directory to download into

            for filefiltered in filtered:
                full_string = (
                    path_string + filefiltered
                )  # Putting together the full URL
                yield {
                    "model": model,
                    "scenario": scenario,
                    "memberid": memberid,
                    "variable": self.variable,
                    "url": full_string,
                    "s3_key": directory + filefiltered,
                }


def model_for_cursor(models, cursor) -> tuple[str, str]:
    """
    Given a cursor, find the next model/scenario to process
    :param models:
    :param cursor:
    :return a tuple of model, scenario:
    """
    # Sort the models so we can chunk on model/scenario name
    model_cursors = sorted(
        f"{model}/{scenario}"
        for model, scenarios in models.items()
        for scenario in scenarios.keys()
    )

    # Skip past model/scenarios we have processed previously
    for model_scan in model_cursors:
        if not cursor or model_scan > cursor:
            model, scenario = model_scan.split("/")
            return model, scenario

    # Cursor is at the end of the list
    return None, None


def run_request(file: dict[str, str],
                model: str, scenario: str,
                monthly: bool) -> RunRequest:
    """
    Construct a RunRequest for a given file
    :param file:
    :param model:
    :param scenario:
    :param monthly:
    :return:
    """
    return RunRequest(
        run_key=file["s3_key"],
        run_config=RunConfig(
            {
                "RawLOCA2": {
                    "config": {
                        "url": file["url"],
                        "s3_key": "/monthly" + file["s3_key"]
                        if monthly else file["s3_key"],
                    }
                },
            }
        ),
        tags={"model": model,
              "scenario": scenario,
              "memberid": file["memberid"],
              "variable": file['variable']
              }
    )


def sensor_implementation(context, models,
                          dataset_resource, monthly: bool) -> Iterable[RunRequest]:
    """
    Implements a common pattern for sensors that iterate over a set of models
     and scenarios The actual sensor needs to be a function with the @sensor
    decorator and performs the yields to feed the run requests to the Dagster engine
    :param context:
    :param models:
    :param dataset_resource:
    :param monthly:
    :return:
    """

    # Find the next model/scenario to process
    model, scenario = model_for_cursor(models, context.cursor)

    if not model:
        return

    # Now we can launch jobs for each of the files for this model/scenario combination
    for file in dataset_resource.get_downloadable_files(
        models, model, scenario, monthly=True
    ):
        context.log.info(f"Found file: {file['url']}")
        yield run_request(file, model, scenario, monthly=monthly)

    context.update_cursor(f"{model}/{scenario}")


@sensor(
    name="LOCA2_Sensor_tasmax",
    target=LOCA2_ASSETS,
    minimum_interval_seconds=LOCA2_SENSOR_FREQUENCY,
    tags={
        "variable": "tasmax",
        "frequency": "daily"})
def loca2_sensor_tasmax(
    context: SensorEvaluationContext,
    loca2_models: Loca2Models,
    loca2_datasets_tasmax: Loca2Datasets,
):
    for request in sensor_implementation(
            context, loca2_models.models, loca2_datasets_tasmax, monthly=False):
        yield request


@sensor(
    name="LOCA2_Sensor_tasmin",
    target=LOCA2_ASSETS,
    minimum_interval_seconds=LOCA2_SENSOR_FREQUENCY,
    tags={
        "variable": "tasmin",
        "frequency": "daily"})
def loca2_sensor_tasmin(
    context: SensorEvaluationContext,
    loca2_models: Loca2Models,
    loca2_datasets_tasmin: Loca2Datasets,
):
    for request in sensor_implementation(
            context, loca2_models.models, loca2_datasets_tasmin, monthly=False):
        yield request


@sensor(
    name="LOCA2_Sensor_pr",
    target=LOCA2_ASSETS,
    minimum_interval_seconds=LOCA2_SENSOR_FREQUENCY,
    tags={
        "variable": "pr",
        "frequency": "daily"})
def loca2_sensor_pr(
    context: SensorEvaluationContext,
    loca2_models: Loca2Models,
    loca2_datasets_pr: Loca2Datasets,
):
    for request in sensor_implementation(
            context, loca2_models.models, loca2_datasets_pr, monthly=False):
        yield request


@sensor(
    name="LOCA2_Sensor_Monthly_tasmax",
    target=LOCA2_ASSETS,
    minimum_interval_seconds=LOCA2_MONTHLY_SENSOR_FREQUENCY,
    tags={
        "variable": "tasmax",
        "frequency": "monthly", })
def loca2_sensor_monthly_tasmax(
    context: SensorEvaluationContext,
    loca2_models: Loca2Models,
    loca2_datasets_tasmax: Loca2Datasets,
):

    for request in sensor_implementation(
            context, loca2_models.models, loca2_datasets_tasmax, monthly=True):
        yield request


@sensor(
    name="LOCA2_Sensor_Monthly_tasmin",
    target=LOCA2_ASSETS,
    minimum_interval_seconds=LOCA2_MONTHLY_SENSOR_FREQUENCY,
    tags={
        "variable": "tasmin",
        "frequency": "monthly", })
def loca2_sensor_monthly_tasmin(
    context: SensorEvaluationContext,
    loca2_models: Loca2Models,
    loca2_datasets_tasmin: Loca2Datasets,
):

    for request in sensor_implementation(
            context, loca2_models.models, loca2_datasets_tasmin, monthly=True):
        yield request


@sensor(
    name="LOCA2_Sensor_Monthly_pr",
    target=LOCA2_ASSETS,
    minimum_interval_seconds=LOCA2_MONTHLY_SENSOR_FREQUENCY,
    tags={
        "variable": "pr",
        "frequency": "monthly", })
def loca2_sensor_monthly_pr(
    context: SensorEvaluationContext,
    loca2_models: Loca2Models,
    loca2_datasets_pr: Loca2Datasets,
):

    for request in sensor_implementation(
            context, loca2_models.models, loca2_datasets_pr, monthly=True):
        yield request
