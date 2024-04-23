from test_resources_utils import create_test_files

# goal is to figure out how much does it take to walk through different projects with different folder structure
test_folder_dir = "/workspaces/schematic_profiler/test_files"

dataset_id, project_id, asset_view_id = create_test_files(
    num_file=10,
    project_name="API test project - folder structure 1",
    test_folder_path=test_folder_dir,
)
print('dataset id', dataset_id)

