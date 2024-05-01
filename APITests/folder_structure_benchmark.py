import time

from synapseutils import walk

from test_resources_utils import (
    create_multi_layer_test_folders,
    create_multi_layer_test_folders_two,
)
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
        walked_path = walk(syn, project_id, includeTypes=["folder", "file"])
        # actually run the function
        _ = list((walked_path))
        end_time = time.time()

        print(f"{i} time trying, and the duration is {end_time - start_time}")
        duration.append(end_time - start_time)

    average_submission_time = sum(duration) / len(duration)

    print(
        f"average time it takes to walk folder after running {repeat} times is: ",
        "{:.2f}".format(average_submission_time),
    )


def get_change(current, previous):
    if current == previous:
        return 0
    try:
        return (abs(current - previous) / previous) * 100.0
    except ZeroDivisionError:
        print("there is something wrong")


# goal is to figure out how much does it take to walk through different projects with different folder structure

# case 1: a dataset folder has 2 layer, and each layer has 2 folders
project_id, asset_view_id = create_multi_layer_test_folders(
    num_folder_per_layer=3,
    num_layer=3,
    project_name="API test project -folder structure 1",
)
# project id: "syn58615911"
calculate_walk_folder_time(project_id="syn58615911", repeat=10)

# # case 2: a dataset folder has 5 layer, and each layer has 5 folders
project_id, asset_view_id = create_multi_layer_test_folders(
    num_folder_per_layer=5,
    num_layer=5,
    project_name="API test project -folder structure 2",
)
# project id: syn58617307
calculate_walk_folder_time(project_id="syn58617307", repeat=5)

# # case 3: a dataset folder has 10 layers, and each layer has 2 folders
project_id, asset_view_id = create_multi_layer_test_folders(
    num_folder_per_layer=2,
    num_layer=10,
    project_name="API test project -folder structure 3",
)
# project id: syn58621243
calculate_walk_folder_time(project_id="syn58621243", repeat=10)

# keep the number of folders constant

# case 1: one layer with 10 folders
project_id, asset_view_id = create_multi_layer_test_folders_two(
    first_layer_num=100,
    max_layer=2,
    project_name="API test project - folder structure 4",
    total_entities_to_create=200,
)
calculate_walk_folder_time(project_id="syn58798762", repeat=10)
# project id: syn58798762

# case 2: two layer. First layer has 50 folders.
project_id, asset_view_id = create_multi_layer_test_folders_two(
    first_layer_num=50,
    max_layer=4,
    project_name="API test project - folder structure 5",
    total_entities_to_create=200,
)
calculate_walk_folder_time(project_id="syn58799439", repeat=10)
# project id: syn58799439

# case 3:
project_id, asset_view_id = create_multi_layer_test_folders_two(
    first_layer_num=25,
    max_layer=8,
    project_name="API test project - folder structure 6",
    total_entities_to_create=200,
)
calculate_walk_folder_time(project_id="syn58799662", repeat=10)
# project id: syn58799662
