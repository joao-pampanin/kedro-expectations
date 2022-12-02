"""Defines all functions related to Great Expectations Datasources."""
import click
import os
import yaml
import great_expectations as ge


@click.command()
def init() -> None:
    try:
        os.system("great_expectations init -y")

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
        context.list_datasources()

    except yaml.YAMLError as exc:
        print("Error while parsing YAML:\n", exc)
