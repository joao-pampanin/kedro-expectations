"""Implementation of the Kedro Expectations Hooks."""
import os
from typing import Any, Dict, cast, Callable
from kedro.framework.hooks import hook_impl
from great_expectations.exceptions import DataContextError
from kedro_expectations.exceptions import SuiteValidationFailure
from pandas import DataFrame as PandasDataFrame
from pyspark.sql import DataFrame as SparkDataFrame
from kedro_expectations.utils import dot_to_underscore, get_property_from_catalog_item, validate, get_suite_name, get_all_expectations, base_ge_folder_exists, location_is_kedro_root_folder


class KedroExpectationsHooks:
    """Implementation of the Kedro Expectations Hooks."""

    def __init__(self, fail_fast: bool = False) -> None:
        self._fail_fast = fail_fast
        pass

    @hook_impl
    def before_node_run(self, inputs: Dict[str, Any]) -> None:
        """Validate inputs that are supported and have an expectation suite available."""
        if self.before_node_run and base_ge_folder_exists() and location_is_kedro_root_folder():
            self._run_validation(inputs)

    def _run_validation(self, data: Dict[str, Any]) -> None:
        for key, value in data.items():

            if get_property_from_catalog_item(key, "type") == "PartitionedDataSet":
                partitions = cast(Dict[str, Callable], value)

                for casted_key, casted_value in partitions.items():

                    # Looking for an specific expectation
                    adjusted_key_pt1 = dot_to_underscore(key)
                    adjusted_key_pt2 = dot_to_underscore(casted_key)
                    print(f"Minha adjusted_key_pt1 é: {adjusted_key_pt1}")
                    print(f"Minha adjusted_key_pt2 é: {adjusted_key_pt2}")

                    adjusted_key = os.path.join(adjusted_key_pt1, adjusted_key_pt2)
                    all_expectations = get_all_expectations(adjusted_key)

                    ge_adjusted_key = adjusted_key_pt1 + "." + adjusted_key_pt2

                    # Looking for a general expectation
                    if not all_expectations:
                        adjusted_key = dot_to_underscore(key)
                        all_expectations = get_all_expectations(adjusted_key)
                        ge_adjusted_key = adjusted_key

                    for exp_file in all_expectations:
                        suite_name = get_suite_name(exp_file, ge_adjusted_key)
                        result = validate(casted_key, suite_name, casted_value())

                        if self._fail_fast and not result.success:
                            raise SuiteValidationFailure(
                                f"Suite {suite_name} for DataSet {adjusted_key} failed!"
                            )

            else:
                adjusted_key = dot_to_underscore(key)
                all_expectations = get_all_expectations(adjusted_key)
                for exp_file in all_expectations:
                    suite_name = get_suite_name(exp_file, adjusted_key)

                    if isinstance(value, PandasDataFrame):
                        result = validate(adjusted_key, suite_name, value)

                    elif isinstance(value, SparkDataFrame):
                        print("Spark support still not available! Skipping")

                    else:
                        print(f"Dataset {adjusted_key} is not supported by Kedro Expectations")

                    if self._fail_fast and not result.success:
                        raise SuiteValidationFailure(
                            f"Suite {suite_name} for DataSet {adjusted_key} failed!"
                        )
                if not all_expectations:
                    print(
                        f"No expectation suite was found for \"{key}\".",
                        "Validation will be skipped!"
                    )
