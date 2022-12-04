"""Defines all functions related to Great Expectations Datasources."""
import click
import yaml
import great_expectations as ge
from kedro_expectations.utils import base_ge_folder_does_NOT_exist, location_is_kedro_root_folder
from subprocess import Popen, DEVNULL


@click.command()
def init() -> None:
    if location_is_kedro_root_folder() and base_ge_folder_does_NOT_exist():
        init_ge_and_create_datasources()


def init_ge_and_create_datasources() -> None:
    try:
        print("Creating base great_expectations folder...")
        Popen(
            ['echo Y | great_expectations init'],
            shell=True,
            stdout=DEVNULL
        ).wait()
        print("great_expectations folder successfuly created!")

        print("Generating Kedro Expectations Datasource...")

        context = ge.get_context()
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
        # context.list_datasources()
        print("Kedro Expectations successfuly generated!")

    except yaml.YAMLError as exc:
        print("Error while parsing YAML:\n", exc)
