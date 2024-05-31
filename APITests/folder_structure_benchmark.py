import time

from synapseutils import walk


from test_resources_utils import CreateTestFolders
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
        start_time = time.perf_counter()
        walked_path = walk(syn, project_id, includeTypes=["folder", "file"])
        # actually run the function
        walked_path = list((walked_path))
        end_time = time.perf_counter()

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


# # goal is to figure out how much does it take to walk through different projects with different folder structure

# case 1: a dataset folder has 2 layers, and each layer has 3 folders
create_test_folders = CreateTestFolders(max_depth=3)

project_id, _ = create_test_folders.create_multi_layer_test_folders(
    num_folder_per_layer=3,
    project_name="API test project -folder structure 1",
)
# # project id: "syn58918330"
calculate_walk_folder_time(project_id="syn58918330", repeat=10)

# # # case 2: a dataset folder has 5 layer, and each layer has 5 folders
create_test_folders = CreateTestFolders(max_depth=5)
project_id, _ = create_test_folders.create_multi_layer_test_folders(
    num_folder_per_layer=5,
    project_name="API test project -folder structure 2",
)
# # # project id: syn58617307
calculate_walk_folder_time(project_id="syn58617307", repeat=5)

# # # case 3: a dataset folder has 10 layers, and each layer has 2 folders
create_test_folders = CreateTestFolders(max_depth=10)
project_id, _ = create_test_folders.create_multi_layer_test_folders(
    num_folder_per_layer=2,
    project_name="API test project -folder structure 3",
)
#  project id: syn58621243
calculate_walk_folder_time(project_id="syn58621243", repeat=10)

### keep the number of folders constant

## case 1: one layer with 100 folders
create_test_folders = CreateTestFolders(max_depth=2)
project_id, _ = create_test_folders.create_multi_layer_test_folders_two(
    first_layer_num=100,
    project_name="API test project - folder structure 4",
    total_entities_to_create=200,
)
## project id: syn58798762
# calculate_walk_folder_time(project_id="syn58798762", repeat=10)


## case 2: three layers. First layer has 50 folders.
create_test_folders = CreateTestFolders(max_depth=4)
project_id, _ = create_test_folders.create_multi_layer_test_folders_fixed_entities(
    first_layer_num=50,
    project_name="API test project - folder structure 5",
    total_folders_to_create=200,
)
# project id: syn58799439
calculate_walk_folder_time(project_id="syn58799439", repeat=10)

## case 3: eight layers. First layer has 25 folders.
create_test_folders = CreateTestFolders(max_depth=8)
project_id, _ = create_test_folders.create_multi_layer_test_folders_fixed_entities(
    first_layer_num=25,
    project_name="API test project - folder structure 6",
    total_folders_to_create=200,
)
calculate_walk_folder_time(project_id="syn58799662", repeat=10)
## project id: syn58799662


## skeep the number of folders constant and folder structure constant
## case1: 10 files per layer starting from the second layer
## so this has 200 folders plus 3(layers)*10(files per layer)*50=1500 files
create_test_folders = CreateTestFolders(max_depth=4, test_folder_path="test_files")
project_id, _ = create_test_folders.create_multi_layer_test_folders_fixed_entities(
    first_layer_num=50,
    project_name="API test project - folder structure 7",
    total_folders_to_create=200,
    num_files=10,
)
calculate_walk_folder_time(project_id="syn58887872", repeat=10)


## case2: 20 files per layer starting from the second layer
## so this has 200 folders plus 3(layers)*20(files per layer)*50=3000 files
create_test_folders = CreateTestFolders(max_depth=4, test_folder_path="test_files")
project_id, _ = create_test_folders.create_multi_layer_test_folders_fixed_entities(
    first_layer_num=50,
    project_name="API test project - folder structure 8",
    total_folders_to_create=200,
    num_files=20,
)
calculate_walk_folder_time(project_id="syn58890699", repeat=10)

## case3: 40 files per layer starting from the second layer
## so this has 200 folders plus 3(layers)*40(files per layer)*50=6000 files
create_test_folders = CreateTestFolders(max_depth=4, test_folder_path="test_files")
project_id, _ = create_test_folders.create_multi_layer_test_folders_fixed_entities(
    first_layer_num=50,
    project_name="API test project - folder structure 9",
    total_folders_to_create=200,
    num_files=40,
)
calculate_walk_folder_time(project_id="syn58943676", repeat=10)
