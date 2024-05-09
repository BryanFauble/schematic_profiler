from test_resources_utils import CreateTestFolders, CreateTestFiles
import asyncio
from utils import StoreRuntime
from typing import Optional
from utils import send_request

from manifest_generator import GenerateManifest
import logging

logger = logging.getLogger("manifest generation benchmark")

create_test_folders = CreateTestFolders(max_depth=2)


# create a project with 2000 folders
project_id, _ = create_test_folders.create_multi_layer_test_folders(
    project_name="API test project - manifest generate limit - 7",
    num_folder_per_layer=2000,
    ignore_first_layer=True,
)

# added 2000 files
create_test_files_new = CreateTestFiles(
    num_test_files=2000, test_folder_path="test_files"
)
srt = StoreRuntime()
syn = srt.login_synapse()
folder_entity = syn.get("syn59194832")
create_test_files_new.create_local_test_files()
asyncio.run(
    create_test_files_new.store_multi_test_files_on_syn(syn_dataset=folder_entity)
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
asset_view_id = "syn59194831"
dataset_id = "syn59194832"
num_time = 10

execute_manifest_generate_benchmark(
    schema_url=schema_url,
    data_type=data_type,
    num_time=num_time,
    asset_view_id=asset_view_id,
    dataset_id=dataset_id,
)
