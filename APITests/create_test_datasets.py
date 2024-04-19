import asyncio
import logging
import os
import shutil
from pathlib import Path
from synapseclient import EntityViewSchema, EntityViewType, Folder, Project
from synapseclient.models import File
from utils import StoreRuntime, send_manifest, send_post_request

logger = logging.getLogger("test upload annotations parameter")

# login to synapse
srt = StoreRuntime()
syn = srt.login_synapse()


# create a test project
def create_test_project(project_name: str) -> Project:
    """create test project

    Args:
        project_name (str): create test project

    Returns:
        Project: synapse project
    """
    project = Project(name=project_name)
    project = syn.store(obj=project)
    return project, project["id"]


# create a folder under a project
def create_test_folder(project: Project) -> Folder:
    """synapse test folder

    Args:
        project (Project): synapse project

    Returns:
        Folder: synapse folder created
    """
    data_folder = Folder("Test sub folder", parent=project)
    data_folder = syn.store(data_folder)
    return data_folder


def create_test_entity_view(project_syn_id: str, project: Project) -> EntityViewSchema:
    """create test entity view on synapse

    Args:
        project_syn_id (str): synapse project id
        project (Project): synapse project

    Returns:
        EntityViewSchema: entity view created
    """
    scopeIds = [project_syn_id]
    entity_view = EntityViewSchema(
        name="test view",
        scopeIds=scopeIds,
        includeEntityTypes=[EntityViewType.FILE, EntityViewType.FOLDER],
        parent=project,
    )
    entity_view = syn.store(entity_view)
    return entity_view


# maybe changed this file to
def write_test_file(text_to_write: str, file_path: Path) -> None:
    """write mock test files

    Args:
        text_to_write (str): test string to write
        file_path (Path): test file path
    """
    if not os.path.exists(file_path):
        with open(file_path, "w") as file:
            file.write(text_to_write)


# create test files locally
def create_local_test_files(num_test_files: int) -> None:
    """create local test files

    Args:
        num_test_files (int): number of test files to be created locally
    """
    current_directory = os.getcwd()
    test_dir = current_directory + "/test_files"
    if not os.path.exists(test_dir):
        os.makedirs(test_dir)

    files = os.listdir(test_dir)
    current_num_files = len(files)
    if current_num_files < num_test_files:
        num_files_to_add = num_test_files - len(files)

        for i in range(current_num_files, current_num_files + num_files_to_add):
            sample_file = f"{test_dir}/sample_file_{i}.txt"
            write_test_file("writing test files", sample_file)


async def store_test_files_on_syn(syn_dataset: Folder) -> None:
    """asynchronously store test files on synapse

    Args:
        syn_dataset (Folder): synapse dataset folder
    """
    current_directory = os.getcwd()
    test_dir = current_directory + "/test_files"
    files = os.listdir(test_dir)

    # store the files async
    # assuming that we are storing all the test files
    for test_file in files:
        path_test_file = current_directory + "/test_files/" + test_file
        file = File(path=path_test_file)
        await file.store_async(parent=syn_dataset)


def create_test_files(num_file: int) -> None:
    """create test files in a given folder

    Args:
        num_file (int): number of test files to create in a folder
    """
    project, project_id = create_test_project("My testing project")
    data_folder = create_test_folder(project)
    entity_view = create_test_entity_view(project_syn_id=project_id, project=project)
    create_local_test_files(num_file)
    asyncio.run(store_test_files_on_syn(data_folder))

    return data_folder.id, project_id, entity_view.id


def execute_submission_comparison(
    dataset_id: str, asset_view_id: str, file_path_manifest: Path, num_time: int
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
    file_annotations_upload_lst = [False]
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
        for count in range(num_time):
            dt_string, time_diff, all_status_code = send_post_request(
                base_url, params, 1, send_manifest, file_path_manifest, headers
            )
            if all_status_code["200"] != 1:
                print("encountered an error when submitting the manifest")
            else:
                submission_duration.append(time_diff)
        average_submission_time = sum(submission_duration) / len(submission_duration)
        logger.info(
            f"average time of submitting a manifest when upload_file_annotations set to {i} is: {average_submission_time}"
        )


# utils function that are useful in testing process
def clean_up_tests(item: str):
    if os.path.exists(item):
        try:
            if os.path.isdir(item):
                shutil.rmtree(item)
            else:  # remove the file
                os.remove(item)
        except Exception as ex:
            print(ex)


dataset_id, project_id, asset_view_id = create_test_files(1000)
file_path_manifest = "/Users/lpeng/Downloads/test_bulkrna-seq.csv"

# dataset_id = "syn57430952"
# project_id = "syn57429449"
# asset_view_id = "syn57429597"
execute_submission_comparison(dataset_id, asset_view_id, file_path_manifest, 10)
