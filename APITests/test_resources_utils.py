import asyncio
import logging
import os
import shutil
from pathlib import Path
from synapseclient import EntityViewSchema, EntityViewType, Folder, Project
from synapseclient.models import File
from utils import StoreRuntime

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
def write_test_file(text_to_write: str, file_path: str) -> None:
    """write mock test files

    Args:
        text_to_write (str): test string to write
        file_path (str): test file path
    """
    if not os.path.exists(file_path):
        with open(file_path, "w") as file:
            file.write(text_to_write)


# create test files locally
def create_local_test_files(num_test_files: int, test_folder_path: str) -> None:
    """create local test files

    Args:
        num_test_files (int): number of test files to be created locally
        test_folder_path (str): path of local test folder
    """
    test_dir = test_folder_path
    if not os.path.exists(test_dir):
        os.makedirs(test_dir)

    files = os.listdir(test_dir)
    current_num_files = len(files)
    if current_num_files < num_test_files:
        num_files_to_add = num_test_files - len(files)

        for i in range(current_num_files, current_num_files + num_files_to_add):
            sample_file = f"{test_dir}/sample_file_{i}.txt"
            write_test_file("writing test files", sample_file)


async def store_test_file_on_syn(
    syn_dataset: Folder, test_file: str, test_folder: Path
) -> None:
    """asynchronously store test files on synapse

    Args:
        syn_dataset (Folder): synapse dataset folder
        test_file (Str): test file name
        test_folder (str): path of test folder
    """
    path_test_file = test_folder + "/" + test_file
    file = File(path=path_test_file)
    await file.store_async(parent=syn_dataset)


async def store_multi_test_files_on_syn(syn_dataset: Folder, test_folder: str) -> None:
    """store multiple test files on synapse

    Args:
        test_folder (str): store multiple test files on synapse
    """
    # assume directory is called "test files"
    files = os.listdir(test_folder)
    for test_file in files:
        task = store_test_file_on_syn(
            syn_dataset=syn_dataset, test_folder=test_folder, test_file=test_file
        )
        await asyncio.gather(task)


def create_test_files(num_file: int, project_name: str, test_folder_path: str) -> None:
    """create test files in a given folder

    Args:
        num_file (int): number of test files to create in a folder
        project_name (str): name of project on synapse
        test_folder_path (str): path of local test folder
    """
    project, project_id = create_test_project(project_name)
    data_folder = create_test_folder(project)
    entity_view = create_test_entity_view(project_syn_id=project_id, project=project)
    create_local_test_files(num_file, test_folder_path)
    asyncio.run(
        store_multi_test_files_on_syn(
            syn_dataset=data_folder, test_folder=test_folder_path
        )
    )

    return data_folder.id, project_id, entity_view.id

def create_test_folder_

def clean_up_tests(item: str):
    if os.path.exists(item):
        try:
            if os.path.isdir(item):
                shutil.rmtree(item)
            else:  # remove the file
                os.remove(item)
        except Exception as ex:
            print(ex)