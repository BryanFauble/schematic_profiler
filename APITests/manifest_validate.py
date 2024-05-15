from dataclasses import dataclass
from typing import Tuple
import logging
from utils import (
    BASE_URL,
    EXAMPLE_SCHEMA_URL,
    HTAN_SCHEMA_URL,
    Row,
    MultiRow,
    StoreRuntime,
    FormatPerformanceOutput,
    CalculateRunTime,
)

CONCURRENT_THREADS = 1

logger = logging.getLogger("manifest-validate")


@dataclass
class ManifestValidate:
    url: str
    token: str = StoreRuntime.get_access_token()

    def __post_init__(self):
        self.params: dict = {
            "schema_url": self.url,
        }
        self.headers = {"Authorization": f"Bearer {self.token}"}
        self.base_url = f"{BASE_URL}/model/validate"
        self.cal_run_time = CalculateRunTime(url=self.base_url, headers=self.headers)

    def validate_example_data_manifest(self) -> MultiRow:
        """
        validating an example data manifest
        """
        params = self.params
        # update parameter. For this example, validate a Patient manifest
        params["data_type"] = "Patient"

        restrict_rules_opt = [True, False]
        # calculate latency of running /model/validate with different parameters
        combined_results = []
        for opt in restrict_rules_opt:
            params["restrict_rules"] = opt

            dt_string, time_diff, status_code_dict = self.cal_run_time.send_request(
                params,
                CONCURRENT_THREADS,
                manifest_to_send_func=self.cal_run_time.send_manifest,
                file_path_manifest="test_manifests/synapse_storage_manifest_patient.csv",
            )

            result = FormatPerformanceOutput.format_run_time_result(
                endpoint_name="model/validate",
                description=f"Validate an example data model using the patient component with restrict_rules set to {opt}. The manifest has 600 rows.",
                data_schema="example data schema",
                num_rows=600,  # number of rows of the manifest being validated
                data_type=params["data_type"],
                restrict_rules=opt,
                dt_string=dt_string,
                num_concurrent=CONCURRENT_THREADS,
                latency=time_diff,
                status_code_dict=status_code_dict,
            )

            combined_results.append(result)

        return combined_results

    def validate_HTAN_data_manifest(self) -> Row:
        """
        validating a HTAN manifest
        """
        params = self.params
        # update parameter. For this example, validate a Biospecimen manifest
        params["data_type"] = "Biospecimen"

        dt_string, time_diff, status_code_dict = self.cal_run_time.send_request(
            params=params,
            concurrent_threads=CONCURRENT_THREADS,
            manifest_to_send_func=self.cal_run_time.send_manifest,
            file_path_manifest="test_manifests/synapse_storage_manifest_HTAN_HMS.csv",
        )

        return FormatPerformanceOutput.format_run_time_result(
            endpoint_name="model/validate",
            description="Validate a HTAN data model using the biospecimen component with restrict_rules set to False. The manifest has around 700 rows.",
            data_schema="HTAN data schema",
            num_rows=773,  # number of rows of the manifest being validated
            data_type=params["data_type"],  # data type
            restrict_rules=False,  # Restrict rules is set to False
            dt_string=dt_string,
            num_concurrent=CONCURRENT_THREADS,
            latency=time_diff,
            status_code_dict=status_code_dict,
        )


def monitor_manifest_validator() -> Tuple[Row, Row, Row]:
    logger.info("Monitoring manifest validation")
    vm_example_manifest = ManifestValidate(EXAMPLE_SCHEMA_URL)
    rows = vm_example_manifest.validate_example_data_manifest()
    # parse the results
    row_one = rows[0]
    row_two = rows[1]

    vm_htan_manifest = ManifestValidate(HTAN_SCHEMA_URL)
    row_three = vm_htan_manifest.validate_HTAN_data_manifest()

    return row_one, row_two, row_three


monitor_manifest_validator()
