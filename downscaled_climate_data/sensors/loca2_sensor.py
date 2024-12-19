import fnmatch
import urllib.error
import urllib.request
from typing import Any

from bs4 import BeautifulSoup
from dagster import (sensor, ConfigurableResource,
                     SensorEvaluationContext, EnvVar,
                     RunRequest, RunConfig)

from downscaled_climate_data.assets.loca2 import loca2_raw


class Loca2Models(ConfigurableResource):
    models: dict

    def __init__(self, **data: Any):
        data['models']={"ACCESS-CM2": {"historical": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1"},
                                                "ssp245": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1"},
                                                "ssp370": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1"},
                                                "ssp585": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1"}},
                                 "ACCESS-ESM1-5": {
                                     "historical": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1"},
                                     "ssp245": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1"},
                                     "ssp370": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1"},
                                     "ssp585": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1"}},
                                 "AWI-CM-1-1-MR": {
                                     "historical": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1"},
                                     "ssp245": {"r1i1p1f1"},
                                     "ssp370": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1"},
                                     "ssp585": {"r1i1p1f1"}},
                                 "BCC-CSM2-MR": {"historical": {"r1i1p1f1"}, "ssp245": {"r1i1p1f1"},
                                                 "ssp370": {"r1i1p1f1"},
                                                 "ssp585": {"r1i1p1f1"}},
                                 "CESM2-LENS": {
                                     "historical": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1",
                                                    "r6i1p1f1",
                                                    "r7i1p1f1", "r8i1p1f1", "r9i1p1f1", "r10i1p1f1"},
                                     "ssp370": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1", "r6i1p1f1",
                                                "r7i1p1f1", "r8i1p1f1", "r9i1p1f1", "r10i1p1f1"}},
                                 "CNRM-CM6-1": {"historical": {"r1i1p1f2"}, "ssp245": {"r1i1p1f2"},
                                                "ssp370": {"r1i1p1f2"},
                                                "ssp585": {"r1i1p1f2"}},
                                 "CNRM-CM6-1-HR": {"historical": {"r1i1p1f2"}, "ssp585": {"r1i1p1f2"}},
                                 "CNRM-ESM2-1": {"historical": {"r1i1p1f2"}, "ssp245": {"r1i1p1f2"},
                                                 "ssp370": {"r1i1p1f2"},
                                                 "ssp585": {"r1i1p1f2"}},
                                 "CanESM5": {
                                     "historical": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1",
                                                    "r6i1p1f1",
                                                    "r7i1p1f1"},
                                     "ssp245": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1", "r6i1p1f1",
                                                "r7i1p1f1"},
                                     "ssp370": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1", "r6i1p1f1",
                                                "r7i1p1f1"},
                                     "ssp585": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1", "r6i1p1f1",
                                                "r7i1p1f1"}},
                                 "EC-Earth3": {"historical": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1"},
                                               "ssp245": {"r1i1p1f1", "r2i1p1f1", "r4i1p1f1"},
                                               "ssp370": {"r1i1p1f1", "r4i1p1f1"},
                                               "ssp585": {"r1i1p1f1", "r3i1p1f1", "r4i1p1f1"}},
                                 "EC-Earth3-Veg": {
                                     "historical": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1"},
                                     "ssp245": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1"},
                                     "ssp370": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1"},
                                     "ssp585": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1"}},
                                 "FGOALS-g3": {"historical": {"r1i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1"},
                                               "ssp245": {"r1i1p1f1", "r3i1p1f1", "r4i1p1f1"},
                                               "ssp370": {"r1i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1"},
                                               "ssp585": {"r1i1p1f1", "r3i1p1f1", "r4i1p1f1"}},
                                 "GFDL-CM4": {"historical": {"r1i1p1f1"}, "ssp245": {"r1i1p1f1"},
                                              "ssp585": {"r1i1p1f1"}},
                                 "GFDL-ESM4": {"historical": {"r1i1p1f1"}, "ssp245": {"r1i1p1f1"},
                                               "ssp370": {"r1i1p1f1"},
                                               "ssp585": {"r1i1p1f1"}},
                                 "HadGEM3-GC31-LL": {"historical": {"r1i1p1f3", "r2i1p1f3", "r3i1p1f3"},
                                                     "ssp245": {"r1i1p1f3"},
                                                     "ssp585": {"r1i1p1f3", "r2i1p1f3", "r3i1p1f3"}},
                                 "HadGEM3-GC31-MM": {"historical": {"r1i1p1f3", "r2i1p1f3"},
                                                     "ssp585": {"r1i1p1f3", "r2i1p1f3"}},
                                 "INM-CM4-8": {"historical": {"r1i1p1f1"}, "ssp245": {"r1i1p1f1"},
                                               "ssp370": {"r1i1p1f1"},
                                               "ssp585": {"r1i1p1f1"}},
                                 "INM-CM5-0": {
                                     "historical": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1"},
                                     "ssp245": {"r1i1p1f1"},
                                     "ssp370": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1"},
                                     "ssp585": {"r1i1p1f1"}},
                                 "IPSL-CM6A-LR": {
                                     "historical": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1",
                                                    "r6i1p1f1",
                                                    "r7i1p1f1", "r8i1p1f1", "r9i1p1f1", "r10i1p1f1"},
                                     "ssp245": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1"},
                                     "ssp370": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1", "r6i1p1f1",
                                                "r7i1p1f1", "r8i1p1f1", "r9i1p1f1", "r10i1p1f1"},
                                     "ssp585": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1"}},
                                 "KACE-1-0-G": {"historical": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1"},
                                                "ssp245": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1"},
                                                "ssp370": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1"},
                                                "ssp585": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1"}},
                                 "MIROC6": {"historical": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1"},
                                            "ssp245": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1"},
                                            "ssp370": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1"},
                                            "ssp585": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1"}},
                                 "MPI-ESM1-2-HR": {
                                     "historical": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1",
                                                    "r6i1p1f1",
                                                    "r7i1p1f1", "r8i1p1f1", "r9i1p1f1", "r10i1p1f1"},
                                     "ssp245": {"r1i1p1f1", "r2i1p1f1"},
                                     "ssp370": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1", "r6i1p1f1",
                                                "r7i1p1f1", "r8i1p1f1", "r9i1p1f1", "r10i1p1f1"},
                                     "ssp585": {"r1i1p1f1", "r2i1p1f1"}},
                                 "MPI-ESM1-2-LR": {
                                     "historical": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1",
                                                    "r6i1p1f1",
                                                    "r7i1p1f1", "r8i1p1f1", "r10i1p1f1"},
                                     "ssp245": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1", "r6i1p1f1",
                                                "r7i1p1f1", "r8i1p1f1", "r10i1p1f1"},
                                     "ssp370": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1", "r7i1p1f1",
                                                "r8i1p1f1", "r10i1p1f1"},
                                     "ssp585": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1", "r6i1p1f1",
                                                "r7i1p1f1", "r8i1p1f1", "r10i1p1f1"}},
                                 "MRI-ESM2-0": {
                                     "historical": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1"},
                                     "ssp245": {"r1i1p1f1"},
                                     "ssp370": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1", "r5i1p1f1"},
                                     "ssp585": {"r1i1p1f1"}},
                                 "NorESM2-LM": {"historical": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1"},
                                                "ssp245": {"r1i1p1f1", "r2i1p1f1", "r3i1p1f1"}, "ssp370": {"r1i1p1f1"},
                                                "ssp585": {"r1i1p1f1"}},
                                 "NorESM2-MM": {"historical": {"r1i1p1f1", "r2i1p1f1"},
                                                "ssp245": {"r1i1p1f1", "r2i1p1f1"},
                                                "ssp370": {"r1i1p1f1"}, "ssp585": {"r1i1p1f1"}},
                                 "TaiESM1": {"historical": {"r1i1p1f1"}, "ssp245": {"r1i1p1f1"},
                                             "ssp370": {"r1i1p1f1"}}}
        super().__init__(**data)

class Loca2Datasets(ConfigurableResource):
    variable: str = "tasmax"

    def get_downloadable_files(self, models: dict, model: str, scenario: str):
        for memberid in models[model][scenario]:
            # Putting together the URL of the data location
            path_string = (
                    "https://cirrus.ucsd.edu/~pierce/LOCA2/CONUS_regions_split/" + model + "/cent/0p0625deg/" + memberid + "/" +
                    scenario + "/" + self.variable + "/")
            try:
                path_soup = BeautifulSoup(urllib.request.urlopen(path_string),
                                          'html.parser')  # Parsing the website to look for the download
            except urllib.error.HTTPError as e:
                print("Can't find the path", path_string)
                raise e
            file_list = []
            for file in path_soup.find_all('a'):  # Pulling the links
                file_list.append(file.get('href'))

            file_string = (
                    self.variable + "." + model + "." + scenario + "." + memberid + ".*.LOCA_16thdeg_*.cent.nc")
            filtered = fnmatch.filter(file_list, file_string)  # Looking for specifically the full daily dataset

            directory = ( "/" + model + "/" + scenario + "/") # Pulling out the directory to download into

            for filefiltered in [x for x in filtered if 'monthly' not in x]:
                full_string = path_string + filefiltered  # Putting together the full URL
                yield {
                    "model": model,
                    "scenario": scenario,
                    "memberid": memberid,
                    "variable": self.variable,
                    "url": full_string,
                    "s3_key": directory + filefiltered}


@sensor(target=[loca2_raw],
        name="LOCA2_Sensor")
def loca2_sensor(context: SensorEvaluationContext,
                 loca2_models: Loca2Models,
                 loca2_datasets: Loca2Datasets) -> RunRequest:

    destination_bucket = EnvVar("LOCA2_BUCKET").get_value()

    # Sort the models so we can chunk on model/scenario name
    model_cursors = sorted(f"{model}/{scenario}" for model, scenarios in loca2_models.models.items()
            for scenario in scenarios.keys())

    context.log.info("Sorted models: " + str(model_cursors))
    context.log.info("Last model: " + str(context.cursor))

    # Skip past model/scenarios we have processed previously
    model = None
    scenario = None
    for model_scan in model_cursors:
        if not context.cursor or model_scan > context.cursor:
            model, scenario = model_scan.split("/")
            break

    if not model:
        context.log.info("No new models to process")
        return

    # Now we can launch jobs for each of the files for this model/scenario combination
    for file in loca2_datasets.get_downloadable_files(loca2_models.models, model, scenario):
        context.log.info(f"Found file: {file['url']}")
        yield RunRequest(
            run_key=file['s3_key'],
            run_config=RunConfig({"raw_loca2_data":
                                      {"config":
                                           {"url": file['url'],
                                            "bucket": destination_bucket,
                                            "s3_key": file['s3_key']
                                            }
                                       }
                                  }),
            tags={"model": model, "scenario": scenario, "memberid": file['memberid']})

    context.update_cursor(f"{model}/{scenario}")

