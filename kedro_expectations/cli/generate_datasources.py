import click
import os, yaml
import great_expectations as ge
from great_expectations.cli.datasource import sanitize_yaml_and_save_datasource
from great_expectations import execution_engine
from great_expectations.exceptions.exceptions import ConfigNotFoundError
from kedro_expectations.utils import get_execution_engine_class

@click.command()
def generate_datasources():
    


    current_dir_path = os.getcwd()
    catalog_path = os.path.join(current_dir_path, "conf", "base", "catalog.yml")

    context = ge.get_context()

    with open(catalog_path, "r") as stream:
        try:
            created_datasources = []
            catalog = yaml.safe_load(stream)
            for catalog_item in catalog:
                dataset = catalog[catalog_item]
                parent_path, filename = os.path.split(dataset['filepath']) # data/01_raw, companies
                filename, fileextension = os.path.splitext(filename) # companies, .csv
                dataset_type = dataset['type'] # pandas.CSVDataSet
                execution_engine_class, execution_engine_aux_name = get_execution_engine_class(dataset, fileextension)
                
                if execution_engine_class == None:
                    print("The dataset ", catalog_item, " is currently not supported by Kedro Expectations! A datasource will not be created for this dataset")
                    print("A datasource may still be created if there are supported datasets in this same folder, but they should not be used with ", catalog_item)
                    continue
                
                # TODO - Em vez de yaml, da pra usar uma maneira mais confiável (ver na doc do GE)
                datasource_name = parent_path + "_" + execution_engine_aux_name + "_gedatasource"
                if datasource_name not in created_datasources:
                    example_yaml = f"""
                    name: {datasource_name}
                    class_name: Datasource
                    execution_engine:
                        class_name: {execution_engine_class}
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
                    # TODO - Criar exception para esse teste?
                    #context.test_yaml_config(yaml_config=example_yaml)
                    sanitize_yaml_and_save_datasource(context, example_yaml, overwrite_existing=False)

                    created_datasources.append(datasource_name)
            context.list_datasources()

        except yaml.YAMLError as exc:
            print("Error while parsing YAML:\n", exc)
