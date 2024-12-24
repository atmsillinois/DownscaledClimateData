import fnmatch
import urllib.error
import urllib.request

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


class Loca2Datasets(ConfigurableResource):
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

            file_string = (
                self.variable
                + "."
                + model
                + "."
                + scenario
                + "."
                + memberid
                + ".*.LOCA_16thdeg_*.cent.nc"
            )
            filtered = fnmatch.filter(
                file_list, file_string
            )  # Looking for specifically the full daily dataset

            directory = (
                "/" + model + "/" + scenario + "/"
            )  # Pulling out the directory to download into

            def filter_monthly(filename: str, monthly: bool) -> bool:
                return "monthly" in filename if monthly else "monthly" not in filename

            for filefiltered in [x for x in filtered if filter_monthly(x, monthly)]:
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


def model_for_cursor(models, cursor):
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


def run_request(file: dict[str, str], model: str, scenario: str) -> RunRequest:
    return RunRequest(
        run_key=file["s3_key"],
        run_config=RunConfig(
            {
                "RawLOCA2": {
                    "config": {
                        "url": file["url"],
                        "s3_key": file["s3_key"],
                    }
                },
            }
        ),
        tags={"model": model, "scenario": scenario, "memberid": file["memberid"]},
    )


@sensor(target=[loca2_raw, as_zarr], name="LOCA2_Sensor")
def loca2_sensor(
    context: SensorEvaluationContext,
    loca2_models: Loca2Models,
    loca2_datasets: Loca2Datasets,
) -> RunRequest:

    model, scenario = model_for_cursor(loca2_models.models, context.cursor)

    if not model:
        return

    # Now we can launch jobs for each of the files for this model/scenario combination
    for file in loca2_datasets.get_downloadable_files(
        loca2_models.models, model, scenario, monthly=False
    ):
        context.log.info(f"Found file: {file['url']}")
        yield run_request(file, model, scenario)

    context.update_cursor(f"{model}/{scenario}")


@sensor(target=[loca2_raw, as_zarr], name="LOCA2_Sensor_Monthly")
def loca2_sensor_monthly(
    context: SensorEvaluationContext,
    loca2_models: Loca2Models,
    loca2_datasets: Loca2Datasets,
) -> RunRequest:

    model, scenario = model_for_cursor(loca2_models.models, context.cursor)

    if not model:
        return

    # Now we can launch jobs for each of the files for this model/scenario combination
    for file in loca2_datasets.get_downloadable_files(
        loca2_models.models, model, scenario, monthly=True
    ):
        context.log.info(f"Found file: {file['url']}")
        yield run_request(file, model, scenario)

    context.update_cursor(f"{model}/{scenario}")
