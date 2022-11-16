import os, yaml
from typing import Any, Dict

import great_expectations as ge
from kedro.framework.hooks import hook_impl

from ruamel.yaml import YAML
import datetime
from kedro_expectations.utils import get_execution_engine_class

class KedroExpectationsHooks:
    def __init__(self) -> None:
        pass

    @hook_impl
    def before_node_run(self, inputs: Dict[str, Any]) -> None:
        if self.before_node_run:
            self._run_validation(inputs)


    def _run_validation(self, data: Dict[str, Any]):
        ruamel_yaml = YAML()
        context = ge.get_context()

        # TODO - Esse código de catálogo + pegar file_extension provavelmente é desnecessário

        current_dir_path = os.getcwd()
        catalog_path = os.path.join(current_dir_path, "conf", "base", "catalog.yml")
        for catalog_item in data:
            formatted_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S")
            my_checkpoint_name = str(catalog_item) + "_checkpoint_" + str(formatted_time)

            # TODO - Como atuar quando existir mais de uma suite para um mesmo dataset?
            
            with open(catalog_path, "r") as stream:
                try:
                    catalog = yaml.safe_load(stream)
                    dataset = catalog[catalog_item]
                    parent_path, filename = os.path.split(dataset['filepath']) # data/01_raw, companies.csv
                    filename, fileextension = os.path.splitext(filename) # companies, .csv
                    execution_engine_class, execution_engine_aux_name = get_execution_engine_class(dataset, fileextension)
                    datasource_name = parent_path + "_" + str(execution_engine_aux_name) + "_gedatasource"
                    if execution_engine_class != None:
                        yaml_config = f"""
                        name: {my_checkpoint_name}
                        config_version: 1.0
                        class_name: SimpleCheckpoint
                        run_name_template: Kedro-Great-Run-{formatted_time}
                        validations:
                          - batch_request:
                              datasource_name: {datasource_name}
                              data_connector_name: default_inferred_data_connector_name
                              data_asset_name: {catalog_item}{fileextension}
                              data_connector_query:
                                index: -1
                            expectation_suite_name: {catalog_item}.myexp
                        """

                        # TODO - Exception pra esse teste
                        my_checkpoint = context.test_yaml_config(yaml_config=yaml_config)

                        # print(my_checkpoint.get_config(mode="yaml"))

                        context.add_checkpoint(**ruamel_yaml.load(yaml_config))

                        context.run_checkpoint(checkpoint_name=my_checkpoint_name)
                    else:
                        print(f"The expectation suite \"{catalog_item}.myexp\" was not found, so Kedro Expectations will skip the validation for this datasource")
                except yaml.YAMLError as exc:
                    print("Error while parsing YAML:\n", exc)
                    continue
                except KeyError as key:
                    print(f"The item {catalog_item} is not present in the Data Catalog, so naturally its validation will be skipped")
                    continue