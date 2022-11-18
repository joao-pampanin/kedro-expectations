import click
import great_expectations as ge

@click.command()
def generate_generic_datasource():
    # TODO - Ver o que acontece se eu rodar isso 2x no mesmo projeto
    context = ge.get_context()
    datasource_config = {
        "name": "generic_datasource",
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
    context.add_datasource(**datasource_config)