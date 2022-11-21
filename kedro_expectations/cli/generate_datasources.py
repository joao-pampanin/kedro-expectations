"""Defines all functions related to Great Expectations Datasources."""
import click
import os
import yaml
import great_expectations as ge
from kedro_expectations.utils import get_execution_engine_class


@click.command()
def generate_datasources() -> None:
    """Add all the viable datasources to the great_expectations.yml.

    The code works in two steps:
    1) Generating datasources based on the Kedro DataCatalog

    2) Generating the validation datasource, used to
       validate expectations via Kedro inputs
    """
    current_dir_path = os.getcwd()
    catalog_path = os.path.join(
        current_dir_path,
        "conf",
        "base",
        "catalog.yml"
    )

    context = ge.get_context()

    with open(catalog_path, "r") as stream:
        try:
            created_datasources = []
            catalog = yaml.safe_load(stream)
            for catalog_item in catalog:
                dataset = catalog[catalog_item]
                parent_path, filename = os.path.split(dataset['filepath'])
                filename, fileextension = os.path.splitext(filename)
                exec_eng_class, exec_eng_name = get_execution_engine_class(
                    dataset,
                    fileextension
                )

                if exec_eng_class is None:
                    print(
                        f"The dataset {catalog_item} is currently not ",
                        "supported by Kedro Expectations! A datasource will ",
                        "not be created for this dataset.",
                        "\n",
                        "A datasource may still be created if ",
                        "there are supported datasets in this same folder, ",
                        "but if cannot be used with {catalog_item}!"
                    )
                    continue

                datasource_name = parent_path + "_" + \
                    exec_eng_name + "_gedatasource"

                if datasource_name not in created_datasources:
                    example_yaml = {
                        "name": datasource_name,
                        "class_name": "Datasource",
                        "execution_engine": {
                            "class_name": exec_eng_class
                        },
                        "data_connectors": {
                            "default_inferred_data_connector_name": {
                                "class_name": "InferredAssetFilesystemDataConnector",
                                "base_directory": "../" + parent_path,
                                "default_regex": {
                                    "group_names": ["data_asset_name"],
                                    "pattern": "(.*)",
                                }
                            }
                        },
                        "default_runtime_data_connector_name": {
                            "class_name": "RuntimeDataConnector",
                            "assets": {
                                "my_runtime_asset_name": {
                                    "batch_identifiers": ["runtime_batch_identifier_name"]
                                }
                            }
                        }
                    }

                    context.add_datasource(**example_yaml)
                    created_datasources.append(datasource_name)

            validation_datasource = {
                "name": "validation_datasource",
                "class_name": "Datasource",
                "module_name": "great_expectations.datasource",
                "execution_engine": {
                    "module_name": "great_expectations.execution_engine",
                    "class_name": "PandasExecutionEngine",
                },
                "data_connectors": {
                    "default_runtime_data_connector_name": {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": ["default_identifier_name"],
                    },
                },
            }
            context.add_datasource(**validation_datasource)
            context.list_datasources()

        except yaml.YAMLError as exc:
            print("Error while parsing YAML:\n", exc)
