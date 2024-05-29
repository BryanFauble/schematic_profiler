import asyncio
import logging
import os
import shutil
from typing import List, Tuple, Union, Optional
from dataclasses import dataclass

from synapseclient import EntityViewSchema, EntityViewType, Folder, Project
from synapseclient.models import File

from utils import StoreRuntime

logger = logging.getLogger("test upload annotations parameter")


@dataclass
class CreateSynapseResources:
    def __post_init__(self):
        srt = StoreRuntime()
        self.syn = srt.login_synapse()

    def create_test_project(
        self, project_name: Optional[str] = "My favorite test project"
    ) -> Tuple[Project, str]:
        """create test project

        Args:
            project_name (str): create test project

        Returns:
            Project: synapse project
        """
        syn = self.syn
        project = Project(name=project_name)
        project = syn.store(obj=project)
        return project, project["id"]

    def create_test_folder(
        self, parent: Union[Project, Folder], folder_name: Optional[str] = "test folder"
    ) -> Folder:
        """synapse test folder

        Args:
            parent (Union[Project, Folder]): synapse project
            folder_name (Optional[str], optional): name of dataset folder. Defaults to "test folder".

        Returns:
            Folder: synapse folder created
        """
        syn = self.syn
        data_folder = Folder(folder_name, parent=parent)
        data_folder = syn.store(data_folder)
        return data_folder

    def create_test_entity_view(
        self,
        project_syn_id: str,
        project: Project,
        entity_view_name: Optional[str] = "test view",
    ) -> EntityViewSchema:
        """create test entity view on synapse

        Args:
            project_syn_id (str): synapse project id
            project (Project): synapse project
            entity_view_name (Optional[str], optional): Name of entity view. Defaults to "test view".

        Returns:
            EntityViewSchema: entity view created
        """
        scopeIds = [project_syn_id]
        entity_view = EntityViewSchema(
            name=entity_view_name,
            scopeIds=scopeIds,
            includeEntityTypes=[EntityViewType.FILE, EntityViewType.FOLDER],
            parent=project,
        )
        entity_view = self.syn.store(entity_view)
        return entity_view

    def create_all_basic_resources(
        self,
        project_name: Optional[str] = "My favorite test project",
        folder_name: Optional[str] = "test folder",
        entity_view_name: Optional[str] = "test view",
    ) -> Tuple[Project, str, Folder, EntityViewSchema]:
        """create all basic synapse resources

        Args:
            project_name (Optional[str], optional): Name of synapse test project. Defaults to "My favorite test project".
            folder_name (Optional[str], optional): Name of local test folder. Defaults to "test folder".
            entity_view_name (Optional[str], optional): Name of entity view. Defaults to "test view".

        Returns:
            Tuple[Project, str, Folder, EntityViewSchema]: synapse project, the project id, the data folder, and entity view
        """
        project, project_id = self.create_test_project(project_name=project_name)
        data_folder = self.create_test_folder(project, folder_name=folder_name)
        entity_view = self.create_test_entity_view(
            project_syn_id=project_id,
            project=project,
            entity_view_name=entity_view_name,
        )
        return project, project_id, data_folder, entity_view


@dataclass
class CreateTestFiles:
    num_test_files: int
    text_to_write: str = "writing test files"
    test_folder_path: Optional[str] = None

    def write_test_file(self, file_path: str) -> None:
        """write mock test files"""
        if not os.path.exists(file_path):
            with open(file_path, "w") as file:
                file.write(self.text_to_write)

    # create test files locally
    def create_local_test_files(self) -> None:
        """create local test files"""
        test_dir = self.test_folder_path

        if not test_dir:
            raise ValueError(f"{test_dir} that you provided does not exist")

        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)

        os.makedirs(test_dir)

        files = os.listdir(test_dir)
        current_num_files = len(files)

        if current_num_files < self.num_test_files:
            num_files_to_add = self.num_test_files - len(files)

            for i in range(current_num_files, current_num_files + num_files_to_add):
                sample_file = f"{test_dir}/sample_file_{i}.txt"
                self.write_test_file(sample_file)

    async def store_test_file_on_syn(
        self,
        syn_dataset: Folder,
        test_file: str,
    ) -> None:
        """asynchronously store test files on synapse

        Args:
            syn_dataset (Folder): synapse dataset folder
            test_file (Str): test file name
        """
        test_folder = self.test_folder_path

        if not test_folder:
            raise ValueError("Test folder path is not set.")

        path_test_file = test_folder + "/" + test_file
        file = File(path=path_test_file)
        await file.store_async(parent=syn_dataset)

    async def store_multi_test_files_on_syn(self, syn_dataset: Folder) -> None:
        """store multiple test files on synapse

        Args:
            syn_dataset (Folder): synapse dataset Folder
        """
        # assume directory is called "test files"
        files = os.listdir(self.test_folder_path)
        for test_file in files:
            task = self.store_test_file_on_syn(
                syn_dataset=syn_dataset, test_file=test_file
            )
            await asyncio.gather(task)

    @staticmethod
    def create_test_files(
        num_file: int,
        project_name: str,
        test_folder_name: str,
        text_to_write: str = "writing test files",
        entity_view: str = "test view",
    ) -> Tuple[str, str, str]:
        """create test files in a given folder

        Args:
            num_file (int): number of test files to create in a folder
            project_name (str): name of project on synapse
            test_folder_path (str): path of local test folder
            entity_view (Optional[str]): name of entity view
            text_to_write(Optional[str]): text to write in test files.
        Returns:
            Tuple[str, str, str]: data folder id, project id, asset view id
        """
        cyr = CreateSynapseResources()
        (
            _,
            project_id,
            data_folder,
            entity_view_new,
        ) = cyr.create_all_basic_resources(
            project_name=project_name,
            folder_name=test_folder_name,
            entity_view_name=entity_view,
        )

        ctf = CreateTestFiles(
            num_test_files=num_file,
            test_folder_path=test_folder_name,
            text_to_write=text_to_write,
        )
        ctf.create_local_test_files()

        asyncio.run(ctf.store_multi_test_files_on_syn(syn_dataset=data_folder))

        return data_folder.id, project_id, entity_view_new.id


@dataclass
class CreateTestFolders:
    max_depth: int
    test_folder_path: Optional[str] = None

    def __post_init__(self):
        self.create_synapse_resource = CreateSynapseResources()

    def create_test_folder_recursive(
        self,
        next_levels_test_folder: List[Folder],
        num_folder_per_layer: int,
        max_depth: int,
    ) -> None:
        """create number of test folders with a certain number of folders per layer

        Args:
            next_levels_test_folder (List[Folder]): next level of folder
            num_folder_per_layer (int): number of folders per layer
            max_depth (int): maximum depth of folders
        """
        if max_depth == 0:
            return
        new_levels_test_folder = []
        for i in range(num_folder_per_layer):
            for parent in next_levels_test_folder:
                sub_data_folder = self.create_synapse_resource.create_test_folder(
                    parent=parent, folder_name=f"Test folder {i}"
                )
                new_levels_test_folder.append(sub_data_folder)

        self.create_test_folder_recursive(
            num_folder_per_layer=num_folder_per_layer,
            next_levels_test_folder=new_levels_test_folder,
            max_depth=max_depth - 1,
        )

    def create_multi_layer_test_folders(
        self,
        num_folder_per_layer: int,
        project_name: str,
    ) -> Tuple[str, str]:
        """create multiple layers of test folder for testing

        Args:
            num_folder_per_layer (int): number of folder per layer
            project_name (str): project name

        Returns:
            Tuple[str, str]: project id, asset view id
        """

        project, project_id = self.create_synapse_resource.create_test_project(
            project_name
        )
        entity_view = self.create_synapse_resource.create_test_entity_view(
            project_syn_id=project_id, project=project
        )

        first_layer_folder = []

        # create the first layer
        for i in range(num_folder_per_layer):
            sub_data_folder = self.create_synapse_resource.create_test_folder(
                parent=project, folder_name=f"Test folder {i}"
            )
            first_layer_folder.append(sub_data_folder)

        self.create_test_folder_recursive(
            num_folder_per_layer=num_folder_per_layer,
            next_levels_test_folder=first_layer_folder,
            max_depth=self.max_depth - 1,
        )
        return project_id, entity_view.id

    def create_multi_layer_test_folders_fixed_entities(
        self,
        first_layer_num: int,
        project_name: str,
        total_folders_to_create: int,
        num_files: int = 0,
    ) -> Tuple[str, str]:
        """create multiple layers of test folders with fixed total number of folders

        Args:
            first_layer_num (int): number of folders in the first layer
            project_name (str): name of project
            total_folders_to_create (int): total number of folders to create
            num_files (Optional[int]): number of files attached to folders in each layer (except the first layer)

        Returns:
            Tuple[str, str]: project id, entity view id
        """
        project, project_id = self.create_synapse_resource.create_test_project(
            project_name
        )
        entity_view = self.create_synapse_resource.create_test_entity_view(
            project_syn_id=project_id, project=project
        )

        first_layer_folder = []

        # create the first layer
        for i in range(first_layer_num):
            sub_data_folder = self.create_synapse_resource.create_test_folder(
                parent=project, folder_name=f"Test folder {i}"
            )
            first_layer_folder.append(sub_data_folder)

        self.create_test_folder_fixed_entities_recursive(
            max_depth=self.max_depth - 1,
            next_levels_test_folder=first_layer_folder,
            remained_entities_to_create=total_folders_to_create - first_layer_num,
            num_files=num_files,
            test_folder_path=self.test_folder_path,
        )

        return project_id, entity_view.id

    def create_test_folder_fixed_entities_recursive(
        self,
        max_depth: int,
        next_levels_test_folder: List[Folder],
        remained_entities_to_create: int,
        num_files: int = 0,
        test_folder_path: Optional[str] = None,
    ) -> None:
        """create number of test folders for a fixed number of entities and layers

        Args:
            max_depth (int): max depth of folders
            next_levels_test_folder (List[Folder]): list of folders of the next level
            remained_entities_to_create (int): remaining number of folders to create
            num_files (Optional[int], optional): number of files to attach to the next layer of folders. Defaults to 0.
            test_folder_path (Optional[str], optional): if attaching files, specify the local test folder that stores the files. Defaults to None.
        """
        if max_depth == 0 or remained_entities_to_create == 0:
            return

        new_levels_test_folder = []

        for parent in next_levels_test_folder:
            sub_data_folder = self.create_synapse_resource.create_test_folder(
                parent=parent, folder_name="Test folder"
            )
            new_levels_test_folder.append(sub_data_folder)
            remained_entities_to_create = remained_entities_to_create - 1
            if num_files > 0 and test_folder_path:
                ctf = CreateTestFiles(
                    num_test_files=num_files, test_folder_path=self.test_folder_path
                )
                ctf.create_local_test_files()
                asyncio.run(
                    ctf.store_multi_test_files_on_syn(syn_dataset=sub_data_folder)
                )

        self.create_test_folder_fixed_entities_recursive(
            max_depth=max_depth - 1,
            next_levels_test_folder=new_levels_test_folder,
            remained_entities_to_create=remained_entities_to_create,
            num_files=num_files,
            test_folder_path=self.test_folder_path,
        )


def clean_up_tests(item: str):
    if os.path.exists(item):
        try:
            if os.path.isdir(item):
                shutil.rmtree(item)
            else:  # remove the file
                os.remove(item)
        except Exception as ex:
            print(ex)
