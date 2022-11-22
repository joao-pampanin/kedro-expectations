"""Implementation of the Kedro Expectations Hooks."""
from typing import Any, Dict
import great_expectations as ge
from kedro.framework.hooks import hook_impl

import datetime
from great_expectations.exceptions import DataContextError
from kedro_expectations.exceptions import SuiteValidationFailure
from pandas import DataFrame as PandasDataFrame
from pyspark.sql import DataFrame as SparkDataFrame


class KedroExpectationsHooks:
    """Implementation of the Kedro Expectations Hooks."""
    def __init__(self, fail_fast: bool = False) -> None:
        self._fail_fast = fail_fast
        pass

    @hook_impl
    def before_node_run(self, inputs: Dict[str, Any]) -> None:
        """Validate inputs that are supported and have an expectation suite available"""
        if self.before_node_run:
            self._run_validation(inputs)

    def _run_validation(self, data: Dict[str, Any]) -> None:
        for key, value in data.items():
            try:
                if isinstance(value, PandasDataFrame):
                    context = ge.get_context()
                    formatted_time = datetime.datetime.now(
                        datetime.timezone.utc
                    ).strftime("%Y%m%dT%H%M%S")

                    checkpoint_config = {
                        "name": "my_missing_keys_checkpoint",
                        "config_version": 1,
                        "class_name": "SimpleCheckpoint",
                        "validations": [
                            {
                                "batch_request": {
                                    "datasource_name": "validation_datasource",
                                    "data_connector_name": "default_runtime_data_connector_name",
                                    "data_asset_name": key,
                                },
                                "expectation_suite_name": key + ".myexp",
                            }
                        ],
                    }
                    context.add_checkpoint(**checkpoint_config)

                    validation_result = context.run_checkpoint(
                        run_name="Kedro-Expectations-Run"+formatted_time,
                        checkpoint_name="my_missing_keys_checkpoint",
                        batch_request={
                            "runtime_parameters": {"batch_data": value},
                            "batch_identifiers": {
                                "default_identifier_name": "default_identifier"
                            },
                        },
                    )
                    
                    if self._fail_fast and not validation_result.success:
                        raise SuiteValidationFailure(
                            f"Suite {key}.myexp for DataSet {key} failed!"
                        )

                elif isinstance(value, SparkDataFrame):
                    print("Support for Spark Dataframes still not available!")
            except DataContextError:
                print(
                    f"No expectation suite was found for \"{key}\", so ",
                    "the plugin will skip the validation for this datasource"
                )
                continue
