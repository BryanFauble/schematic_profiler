import concurrent.futures
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Callable, Tuple, List, Union, Optional

import pytz
import requests
import synapseclient
from requests import Response
from requests.exceptions import InvalidSchema
from synapseclient import Table


# Create a custom formatter with colors
class ColoredFormatter(logging.Formatter):
    COLORS = {
        "DEBUG": "\033[94m",  # Blue
        "INFO": "\033[92m",  # Green
        "WARNING": "\033[93m",  # Yellow
        "ERROR": "\033[91m",  # Red
        "CRITICAL": "\033[41m\033[97m",  # White text on Red background
        "RESET": "\033[0m",  # Reset color
    }

    def format(self, record):
        log_level_color = self.COLORS.get(record.levelname, self.COLORS["RESET"])
        log_message = super().format(record)
        return f"{log_level_color}{log_message}{self.COLORS['RESET']}"


# Create console handler with colored formatter
console_handler = logging.StreamHandler()
console_handler.setFormatter(
    ColoredFormatter(
        "%(levelname)s: [%(asctime)s] %(name)s" " - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
)

# Add console handler to the logger
logger = logging.getLogger("")
logger.setLevel(logging.INFO)
logger.addHandler(console_handler)


# define variables that will be used across scripts
EXAMPLE_SCHEMA_URL = "https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld"
HTAN_SCHEMA_URL = (
    "https://raw.githubusercontent.com/ncihtan/data-models/main/HTAN.model.jsonld"
)

DATA_FLOW_SCHEMA_URL = "https://raw.githubusercontent.com/Sage-Bionetworks/data_flow/main/inst/data_model/dataflow_component.csv"

BASE_URL = "https://schematic-dev.api.sagebionetworks.org/v1"

# define type Row
Row = List[Union[str, int, dict, bool]]
MultiRow = List[Row]


def fetch(url: str, params: dict, headers: dict = None) -> Response:
    """
    Trigger a get request
    Args:
        url (str): the url to run a given api request
        params (dict): parameter of running a given api request
        headers (dict): headers used for API requests. For example, authorization headers.
    Returns:
        Response: a response object
    """
    response = requests.get(url, params=params, headers=headers)
    return response


def send_manifest(
    url: str, params: dict, headers: dict = None, manifest_path=None
) -> Response:
    """Send an API request to an endpoint
    Args:
        url (str): the url to run a given api request
        params (dict): parameters of running the post request
        headers (dict): headers used for API requests. For example, authorization headers.
        manifest_path (str): file path of a manifest
    Returns:
        Response: a response object
    """
    wd = os.getcwd()
    test_manifest_path = os.path.join(wd, manifest_path)

    if not os.path.exists(test_manifest_path):
        logger.error(
            "the manifest does not exist. Please provide a valid manifest file path"
        )

    return requests.post(
        url,
        params=params,
        headers=headers,
        files={"file_name": open(test_manifest_path, "rb")},
    )


def send_post_request(
    base_url: str,
    params: dict,
    concurrent_threads: int,
    manifest_to_send_func: Callable[[str, dict], Response],
    file_path_manifest: str,
    headers: dict = None,
) -> Tuple[str, float, dict]:
    """
    sending post requests
    Args:
        params (dict): a dictionary of parameters to send
        base_url (str): url of endpoint
        concurrent_threads (int): number of concurrent threads
        manifest_to_send_func (Callable): a function that sends a post request that upload a manifest to be sent
        headers (dict): headers used for API requests. For example, authorization headers.

    Returns:
        dt_string (str): start time of running the API endpoints.
        time_diff (float): time of finish running all requests.
        all_status_code (dict): dict; a dictionary that records the status code of run.
    Todo:
        specify exception
    """
    try:
        # send request and calculate run time
        dt_string, time_diff, status_code_dict = cal_time_api_call(
            url=base_url,
            params=params,
            concurrent_threads=concurrent_threads,
            request_type="post",
            manifest_to_send_func=manifest_to_send_func,
            file_path_manifest=file_path_manifest,
            headers=headers,
        )
    # TO DO: add more details about raising different exception
    # Should exception based on response type?
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        raise
    return dt_string, time_diff, status_code_dict


def send_request(
    base_url: str, params: dict, concurrent_threads: int, headers: dict = None
) -> Tuple[str, float, dict]:
    """
    sending requests to different endpoint
    Args:
        params (dict): a dictionary of parameters to send
        base_url (str): url of endpoint
        concurrent_threads (int): number of concurrent threads
        headers (dict): headers used for API requests. For example, authorization headers.
    Returns:
        dt_string (str): start time of running the API endpoints.
        time_diff (float): time of finish running all requests.
        all_status_code (dict): dict; a dictionary that records the status code of run.
    """

    try:
        # send request and calculate run time
        dt_string, time_diff, status_code_dict = cal_time_api_call(
            base_url, params, concurrent_threads, headers
        )
    # TO DO: add more details about raising different exception
    # Should exception based on response type?
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        raise
    return dt_string, time_diff, status_code_dict


def return_time_now(name_funct_call: Callable = None) -> str:
    """
    Get the time now
    Args:
        name_funct_call (Callable): name of function call (for logging purposes)
    Returns:
        current time formatted as "%d/%m/%Y %H:%M:%S" as a string
    """
    now = datetime.now(pytz.timezone("US/Eastern"))
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")

    if name_funct_call:
        logger.info(
            f"when running {name_funct_call} function, the time is: {dt_string}"
        )

    return dt_string


def cal_time_api_call(
    url: str,
    params: dict,
    concurrent_threads: int,
    headers: dict = None,
    request_type: Optional[str] = "get",
    manifest_to_send_func: Optional[Callable[[str, dict], Response]] = None,
    file_path_manifest: Optional[str] = None,
) -> Tuple[str, float, dict]:
    """calculate the latency of api calls by sending get requests.

    Args:
        url (str): the url that users want to access
        params (dict): the parameters need to use for the request
        concurrent_threads (int): number of concurrent threads requested by users
        headers (dict, optional):a header of dictionary. Defaults to None.
        request_type (Optional[str], optional): type of request. Defaults to "get". If running post request, please provide send function and also file path of a manifest
        manifest_to_send_func (Optional[Callable[[str, dict], Response]], optional): manifest_to_send_func (Callable): a function that sends a post request that upload a manifest to be sent. Defaults to None.
        file_path_manifest (Optional[str], optional): file path of a manifest. Defaults to None.

    Raises:
        InvalidSchema: Not providing a valid url

    Returns:
        dt_string (str): start time of running the API endpoints.
        time_diff (float): time of finish running all requests.
        all_status_code (dict): dict; a dictionary that records the status code of run.
    """
    start_time = time.time()
    # get time of running the api endpoint
    dt_string = return_time_now()

    # execute concurrent requests
    with ThreadPoolExecutor() as executor:
        if request_type == "get":
            futures = [
                executor.submit(fetch, url, params, headers)
                for x in range(concurrent_threads)
            ]
        else:
            futures = [
                executor.submit(
                    manifest_to_send_func, url, params, headers, file_path_manifest
                )
                for x in range(concurrent_threads)
            ]
        all_status_code = {"200": 0, "500": 0, "503": 0, "504": 0}
        for f in concurrent.futures.as_completed(futures):
            try:
                status_code = f.result().status_code
                status_code_str = str(status_code)
                if status_code_str != "200":
                    logger.error(
                        f"{status_code} error running: {url} with using params {params}"
                    )
                all_status_code[status_code_str] += 1
            except InvalidSchema:
                raise InvalidSchema(
                    f"No connection adapters were found for {url}. Please make sure that your URL is correct. "
                )
    time_diff = round(time.time() - start_time, 2)
    logger.info(f"duration time of running {url}: {time_diff}")
    return dt_string, time_diff, all_status_code


def save_run_time_result(
    endpoint_name: str,
    description: str,
    dt_string: str,
    num_concurrent: int,
    latency: float,
    status_code_dict: dict,
    data_schema: str = None,
    num_rows: int = None,
    data_type: str = None,
    output_format: str = None,
    restrict_rules: bool = None,
    manifest_record_type: str = None,
    asset_view: str = None,
) -> Row:
    """
    Record the result of running an endpoint as a dataframe
    Args:
        endpoint_name (str): name of the endpoint being run
        description (str): more details description of the case being run
        num_concurrent (int): number of concurrent requests
        latency (float): latency of finishing the run
        status_code_dict (dict): dictionary of status code
        dt_string (str): start time of the test
        data_schema (str, optional): default to None. the data schema used by the function
        num_rows (int, optional): default to None. number of rows of a given manifest
        data_type (str, optional): default to None. data type/component being used
        output_format (str, optional): default to None. output format of a given manifest
        restrict_rules (bool, optional): default to None. if restrict_rules parameter gets set to true
        manifest_record_type (str, optional): default to None. Manifest storage type. Four options: file only, file+entities, table+file, table+file+entities
        asset view (str, optional): default to None. asset view of the asset store.
    """
    # get specific number of status code
    num_status_200 = status_code_dict["200"]
    num_status_500 = status_code_dict["500"]
    num_status_504 = status_code_dict["504"]
    num_status_503 = status_code_dict["503"]

    new_row = [
        endpoint_name,
        description,
        data_schema,
        num_rows,
        data_type,
        output_format,
        restrict_rules,
        asset_view,
        dt_string,
        manifest_record_type,
        num_concurrent,
        latency,
        num_status_200,
        num_status_500,
        num_status_504,
        num_status_503,
    ]

    return new_row


class StoreRuntime:
    # store run time result
    @staticmethod
    def get_access_token() -> str:
        """Get access token to use asset store resources
        Returns:
            str: a token to access asset store
        """
        # for running on github action
        if "SYNAPSE_AUTH_TOKEN" in os.environ:
            token = os.environ["SYNAPSE_AUTH_TOKEN"]
            logger.debug("Successfully found synapse access token")
        else:
            token = os.environ["TOKEN"]
        if token is None or "":
            logger.error("Synapse access token is not found")

        return token

    def login_synapse(self) -> synapseclient.Synapse:
        """
        Login to synapse using the token provided
        Returns:
            synapse object
        """
        auth_token = self.get_access_token()
        try:
            syn = synapseclient.Synapse()
            syn.login(authToken=auth_token)
        except (
            synapseclient.core.exceptions.SynapseNoCredentialsError,
            synapseclient.core.exceptions.SynapseHTTPError,
        ) as e:
            raise e
        return syn

    def record_run_time_result_synapse(self, rows: MultiRow) -> None:
        # Load existing data from synapse
        syn = self.login_synapse()

        # get existing table from synapse
        existing_table_schema = syn.get("syn51385540")

        # add new row to table
        syn.store(Table(existing_table_schema, rows))

        logger.info("Finish uploading result to synapse. ")
