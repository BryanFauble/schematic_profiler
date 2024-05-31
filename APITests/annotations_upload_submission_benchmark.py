import logging
from utils import (
    StoreRuntime,
    send_manifest_async,
    execute_requests_async,
    parse_http_requests,
)
from test_resources_utils import CreateTestFiles
import asyncio
from time import perf_counter

logger = logging.getLogger("test upload annotations parameter")


async def execute_submission_comparison(
    dataset_id: str, asset_view_id: str, file_path_manifest: str, num_time: int
) -> None:
    """for this use case, only interested in if file_annotations_upload_lst is set to True or False

    Args:
        dataset_id (str): dataset id on synapse
        asset_view_id (str): asset view id on synapse
        file_path_manifests (Path): path of test manifest for submission
        num_time (int): number of times that submission function runs
    """
    srt = StoreRuntime()
    token = srt.get_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    file_annotations_upload_lst = [True, False]
    base_url = "http://localhost:3001/v1/model/submit"
    params = {
        "manifest_record_type": "file_only",
        "schema_url": "https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld",
        "dataset_id": dataset_id,
        "asset_view": asset_view_id,
        "restrict_rules": False,
        "use_schema_label": "class_label",
        "data_model_labels": "class_label",
    }

    for i in file_annotations_upload_lst:
        submission_duration = []
        params["file_annotations_upload"] = i

        # try submitting x amount of time and calculate average
        for _ in range(num_time):
            start_time = perf_counter()
            requests_to_execute = []
            for i in range(1):
                requests_to_execute.append(
                    asyncio.create_task(
                        send_manifest_async(
                            url=base_url,
                            headers=headers,
                            params=params,
                            manifest_path=file_path_manifest,
                        )
                    )
                )
            http_responses = await execute_requests_async(requests_to_execute)
            all_status_code = parse_http_requests(http_responses)
            time_diff = round(perf_counter() - start_time, 2)
            if all_status_code["200"] != 1:
                print("encountered an error when submitting the manifest")
            else:
                submission_duration.append(time_diff)
        average_submission_time = sum(submission_duration) / len(submission_duration)
        logger.info(
            f"average time of submitting a manifest when upload_file_annotations set to {i} is: {average_submission_time}"
        )


test_folder_dir = "test_files_folder"

dataset_id, project_id, asset_view_id = CreateTestFiles.create_test_files(
    num_file=10,
    project_name="API test project random",
    test_folder_name=test_folder_dir,
)

file_path_manifest = "/file/path/to/test_bulkrna-seq.csv"

dataset_id = "syn57430952"
project_id = "syn57429449"
asset_view_id = "syn57429597"
asyncio.run(
    execute_submission_comparison(dataset_id, asset_view_id, file_path_manifest, 10)
)
