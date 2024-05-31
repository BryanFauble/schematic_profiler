from dataclasses import dataclass
from time import perf_counter
from typing import Tuple
import logging
from utils import (
    Row,
    BASE_URL,
    EXAMPLE_SCHEMA_URL,
    HTAN_SCHEMA_URL,
    StoreRuntime,
    format_run_time_result,
    fetch_async,
    parse_http_requests,
    execute_requests_async,
    return_time_now,
)
import asyncio

CONCURRENT_THREADS = 1

logger = logging.getLogger("manifest-generator")


@dataclass
class GenerateManifest:
    url: str
    use_annotation: bool = False
    token: str = StoreRuntime.get_access_token()
    title: str = "example"
    data_type: str = "Patient"

    def __post_init__(self):
        self.params: dict = {
            "schema_url": self.url,
            "title": self.title,
            "data_type": self.data_type,
            "use_annotations": self.use_annotation,
        }
        self.base_url = f"{BASE_URL}/manifest/generate"
        self.headers = {"Authorization": f"Bearer {self.token}"}

    async def generate_new_manifest_example_model(self) -> Row:
        """
        Generate a new manifest as a google sheet by using the example data model
        """
        start_time = perf_counter()

        requests_to_execute = []
        for _ in range(CONCURRENT_THREADS):
            requests_to_execute.append(
                asyncio.create_task(
                    fetch_async(
                        url=self.base_url,
                        headers=self.headers,
                        params=self.params,
                    )
                )
            )
        http_responses = await execute_requests_async(requests_to_execute)
        status_code_dict = parse_http_requests(http_responses)
        time_diff = round(perf_counter() - start_time, 2)

        return format_run_time_result(
            endpoint_name="manifest/generate",
            description="Generating a manifest as a google sheet by using the example data model",
            data_schema="example data schema",
            data_type=self.data_type,
            output_format="google sheet",
            dt_string=return_time_now(),
            num_concurrent=CONCURRENT_THREADS,
            latency=time_diff,
            status_code_dict=status_code_dict,
        )

    def generate_new_manifest_example_model_excel(self, output_format: str) -> Row:
        """
        Generate a new manifest as an excel spreadsheet by using the example data model
        Args:
            output_format: specify the output of manifest. For this function, the output_format is excel
        """
        params = self.params
        params["output"] = output_format

        dt_string, time_diff, status_code_dict = self.cal_run_time.send_request(
            params=params,
            concurrent_threads=CONCURRENT_THREADS,
        )

        return format_run_time_result(
            endpoint_name="manifest/generate",
            description="Generating a manifest as an excel spreadsheet by using the example data model",
            data_schema="example data schema",
            data_type=self.data_type,
            output_format="excel",
            dt_string=dt_string,
            num_concurrent=CONCURRENT_THREADS,
            latency=time_diff,
            status_code_dict=status_code_dict,
        )

    def generate_new_manifest_HTAN_google_sheet(self) -> Row:
        """
        Generate a new manifest as a google sheet by using the HTAN manifest
        """
        dt_string, time_diff, status_code_dict = self.cal_run_time.send_request(
            params=self.params,
            concurrent_threads=CONCURRENT_THREADS,
        )

        return format_run_time_result(
            endpoint_name="manifest/generate",
            description="Generating a manifest as a google spreadsheet by using the HTAN data model",
            data_schema="HTAN data schema",
            data_type=self.data_type,
            output_format="google sheet",
            dt_string=dt_string,
            num_concurrent=CONCURRENT_THREADS,
            latency=time_diff,
            status_code_dict=status_code_dict,
        )

    def generate_existing_manifest_google_sheet(self) -> Row:
        """
        Generate a new manifest as a google sheet by using the existing manifest
        """
        params = self.params
        params["dataset_id"] = "syn51078367"
        params["asset_view"] = "syn23643253"

        dt_string, time_diff, status_code_dict = self.cal_run_time.send_request(
            concurrent_threads=CONCURRENT_THREADS, params=params
        )

        return format_run_time_result(
            endpoint_name="manifest/generate",
            description="Generating an existing manifest as a google sheet by using the example data model",
            data_schema="example data schema",
            num_rows=542,  # number of rows of the existing manifest
            data_type=self.data_type,
            output_format="google sheet",
            dt_string=dt_string,
            num_concurrent=CONCURRENT_THREADS,
            latency=time_diff,
            status_code_dict=status_code_dict,
        )


async def monitor_manifest_generator() -> Tuple[Row, Row, Row, Row]:
    logger.info("Monitoring manifest generation")
    gm_example = GenerateManifest(EXAMPLE_SCHEMA_URL)
    row_one = await gm_example.generate_new_manifest_example_model()
    row_two = gm_example.generate_new_manifest_example_model_excel("excel")

    row_three = gm_example.generate_existing_manifest_google_sheet()

    gm_htan = GenerateManifest(HTAN_SCHEMA_URL)
    row_four = gm_htan.generate_new_manifest_HTAN_google_sheet()

    return row_one, row_two, row_three, row_four


asyncio.run(monitor_manifest_generator())
