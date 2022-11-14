import click
import os, yaml
import great_expectations as ge
from great_expectations.cli.datasource import sanitize_yaml_and_save_datasource

@click.command()
# @click.option(
#     "--target-directory",
#     "-d",
#     default="./",
#     help="The root of the project directory where you want to initialize Great Expectations.",
# )
# @click.option(
#     "--usage-stats/--no-usage-stats",
#     help="By default, usage statistics are enabled unless you specify the --no-usage-stats flag.",
#     default=True,
# )
def generate_datasources():
    # TODO - checar se estou em uma pasta kedro raiz antes de executar
    current_dir_path = os.getcwd()
    catalog_path = os.path.join(current_dir_path, "conf", "base", "catalog.yml")

    context = ge.get_context()

    with open(catalog_path, "r") as stream:
        try:
            catalog = yaml.safe_load(stream)
            for catalog_item in catalog:
                dataset = catalog[catalog_item]
                parent_path, filename = os.path.split(dataset['filepath']) # data/01_raw, companies.csv
                dataset_type = dataset['type'] # pandas.CSVDataSet

                # TODO - IF ELIF PARA TYPES QUE SERÃO ACEITOS (POSSÍVEL PROBLEMA)
                # TODO - batch GE ver opções
                datasource_name = catalog_item + "_gedatasource"

                example_yaml = f"""
                name: {datasource_name}
                class_name: Datasource
                execution_engine:
                    class_name: PandasExecutionEngine
                data_connectors:
                    default_inferred_data_connector_name:
                        class_name: InferredAssetFilesystemDataConnector
                        base_directory: ../{parent_path}
                        default_regex:
                            group_names:
                                - data_asset_name
                            pattern: (.*)
                    default_runtime_data_connector_name:
                        class_name: RuntimeDataConnector
                        assets:
                            my_runtime_asset_name:
                                batch_identifiers:
                                - runtime_batch_identifier_name
                """

                context.test_yaml_config(yaml_config=example_yaml)

                sanitize_yaml_and_save_datasource(context, example_yaml, overwrite_existing=False)
            context.list_datasources()

        except yaml.YAMLError as exc:
            print("Error while parsing YAML:\n", exc)