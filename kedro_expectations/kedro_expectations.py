from typing import Any, Dict
import pandas as pd
import great_expectations as ge
from kedro.framework.hooks import hook_impl

import datetime
from great_expectations.exceptions import DataContextError

import datetime

import pandas as pd

import great_expectations as ge
from great_expectations.exceptions import DataContextError


class KedroExpectationsHooks:
    def __init__(self) -> None:
        pass

    @hook_impl
    def before_node_run(self, inputs: Dict[str, Any]) -> None:
        if self.before_node_run:
            self._run_validation(inputs)


    def _run_validation(self, data: Dict[str, Any]):
        for key, value in data.items():
            try:
                if isinstance(value, pd.DataFrame):
                    context = ge.get_context()
                    formatted_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S")

                    checkpoint_config = {
                        "name": "my_missing_keys_checkpoint",
                        "config_version": 1,
                        "class_name": "SimpleCheckpoint",
                        "validations": [
                            {
                                "batch_request": {
                                    "datasource_name": "generic_datasource",
                                    "data_connector_name": "default_runtime_data_connector_name",
                                    "data_asset_name": key,
                                },
                                "expectation_suite_name": key + ".myexp",
                            }
                        ],
                    }
                    context.add_checkpoint(**checkpoint_config)

                    results = context.run_checkpoint(
                        run_name="Kedro-Expectations-Run"+formatted_time,
                        checkpoint_name="my_missing_keys_checkpoint",
                        batch_request={
                            "runtime_parameters": {"batch_data": value},
                            "batch_identifiers": {
                                "default_identifier_name": "default_identifier"
                            },
                        },
                    )
            except DataContextError:
                print(f"No expectation suite was found for \"{key}\", so Kedro Expectations will skip the validation for this datasource")
                continue