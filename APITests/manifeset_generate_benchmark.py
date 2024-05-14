import logging
from typing import Optional

from manifest_generator import GenerateManifest
from test_resources_utils import create_test_files
from utils import send_request

logger = logging.getLogger("test manifest generation")


def execute_manifest_generate_use_annotations_comparison(
    schema_url: str,
    data_type: str,
    num_time: int,
    asset_view_id: Optional[str],
    dataset_id: Optional[str],
) -> None:
    """compare the run time of generating a manifest

    Args:
        schema_url (str): schema url
        data_type (str): data type of the manifest
        num_time (int): number of times that end endpoint needs to run
        asset_view_id (Optional[str]): asset view id
        dataset_id (Optional[str]): dataset id
    """
    use_annotations = [True, False]
    CONCURRENT_THREADS = 1
    base_url = (
        "https://schematic-dev-refactor.api.sagebionetworks.org/v1/manifest/generate"
    )
    generation_duration = []
    for opt in use_annotations:
        gm = GenerateManifest(url=schema_url, use_annotation=opt, data_type=data_type)
        gm.params["asset_view"] = asset_view_id
        gm.params["dataset_id"] = dataset_id

        for count in range(num_time):
            _, time_diff, all_status_code = send_request(
                base_url=base_url,
                params=gm.params,
                concurrent_threads=CONCURRENT_THREADS,
                headers=gm.headers,
            )
            if all_status_code["200"] != 1:
                logger.error("encountered an error when generating the manifest")
            else:
                generation_duration.append(time_diff)
        average_generate_time = sum(generation_duration) / len(generation_duration)
        logger.info(
            f"average time of generating a manifest when use_annotations set to {opt} is: {average_generate_time}"
        )


# case 1: a dataset folder with 10 dataset files
test_folder_dir = (
    "/Users/lpeng/Documents/schematic_profiler/schematic_profiler/test_new_files"
)
dataset_id, project_id, asset_view_id = create_test_files(
    num_file=10,
    project_name="API manifest generate project",
    test_folder_path=test_folder_dir,
)
print("dataset id", dataset_id)
print("project id", project_id)
print("asset view", asset_view_id)

schema_url = "https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld"
data_type = "BulkRNA-seqAssay"
num_time = 10
execute_manifest_generate_use_annotations_comparison(
    schema_url=schema_url,
    data_type=data_type,
    num_time=num_time,
    asset_view_id=asset_view_id,
    dataset_id=dataset_id,
)


# case 2: a dataset folder with 10 dataset files
test_folder_dir = (
    "/Users/lpeng/Documents/schematic_profiler/schematic_profiler/test_new_files"
)
dataset_id, project_id, asset_view_id = create_test_files(
    num_file=100,
    project_name="API manifest generate project 2",
    test_folder_path=test_folder_dir,
)
print("dataset id", dataset_id)
print("project id", project_id)
print("asset view", asset_view_id)

schema_url = "https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld"
data_type = "BulkRNA-seqAssay"
num_time = 10
execute_manifest_generate_use_annotations_comparison(
    schema_url=schema_url,
    data_type=data_type,
    num_time=num_time,
    asset_view_id=asset_view_id,
    dataset_id=dataset_id,
)
