from test_resources_utils import CreateTestFolders
from typing import Optional
from utils import send_request

from manifest_generator import GenerateManifest
import logging

logger = logging.getLogger("manifest generation benchmark")

create_test_folders = CreateTestFolders(max_depth=2, test_folder_path="test_files")


# create a project with 2000 folders

project_id, _ = create_test_folders.create_multi_layer_test_folders_fixed_entities(
    first_layer_num=1,
    project_name="API test project - manifest generate limit - 9",
    num_folder_per_layer=1800,
    num_files=1,
)

project_id, _ = create_test_folders.create_multi_layer_test_folders_fixed_entities(
    first_layer_num=1,
    project_name="API test project - manifest generate limit - 10",
    num_folder_per_layer=2000,
    num_files=1,
)


def execute_manifest_generate_benchmark(
    schema_url: str,
    data_type: str,
    num_time: int,
    asset_view_id: Optional[str],
    dataset_id: Optional[str],
):
    gm = GenerateManifest(url=schema_url, use_annotation=True, data_type=data_type)
    gm.params["asset_view"] = asset_view_id
    gm.params["dataset_id"] = dataset_id
    gm.params["output_format"] = "google_sheet"

    base_url = (
        "https://schematic-dev-refactor.api.sagebionetworks.org/v1/manifest/generate"
    )
    generation_duration = []
    for count in range(num_time):
        _, time_diff, all_status_code = send_request(base_url, gm.params, 1, gm.headers)
        if all_status_code["200"] != 1:
            logger.error("encountered an error when generating the manifest")
        else:
            generation_duration.append(time_diff)
        average_generate_time = sum(generation_duration) / len(generation_duration)
    logger.info(f"average time of generating a manifest is: {average_generate_time}")


schema_url = "https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld"
data_type = "BulkRNA-seqAssay"
asset_view_id = ""
dataset_id = ""
num_time = 10

# execute_manifest_generate_benchmark(
#     schema_url=schema_url,
#     data_type=data_type,
#     num_time=num_time,
#     asset_view_id=asset_view_id,
#     dataset_id=dataset_id,
# )
