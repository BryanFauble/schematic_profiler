import time

from synapseutils import walk

from test_resources_utils import create_multi_layer_test_folders
from utils import StoreRuntime


def calculate_walk_folder_time(project_id: str, repeat: int) -> None:
    """time it takes to walking different folder

    Args:
        project_id (str): synapse project ID
        repeat (int): number of times that you want the walk function to run
    """
    srt = StoreRuntime()
    syn = srt.login_synapse()
    duration = []
    for i in range(repeat):
        start_time = time.time()
        walk(syn, project_id, includeTypes=["folder", "file"])
        end_time = time.time()
        duration.append(end_time - start_time)

    average_submission_time = sum(duration) / len(duration)

    print(
        f"average time it takes to walk folder after running {repeat} times is: ",
        average_submission_time,
    )


# goal is to figure out how much does it take to walk through different projects with different folder structure

# case 1: a dataset folder has 3 layer, and each layer has 3 folders
project_id, asset_view_id = create_multi_layer_test_folders(
    num_folder_per_layer=2,
    num_layer=2,
    project_name="API test project -folder structure 1",
)
calculate_walk_folder_time(project_id=project_id, repeat=10)

# case 2: a dataset folder has 5 layer, and each layer has 5 folders
project_id, asset_view_id = create_multi_layer_test_folders(
    num_folder_per_layer=5,
    num_layer=5,
    project_name="API test project -folder structure 2",
)
calculate_walk_folder_time(project_id=project_id, repeat=10)

# case 3: a dataset folder has 10 layers, and each layer has 2 folders
project_id, asset_view_id = create_multi_layer_test_folders(
    num_folder_per_layer=2,
    num_layer=10,
    project_name="API test project -folder structure 3",
)
calculate_walk_folder_time(project_id=project_id, repeat=10)
