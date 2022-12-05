import os
import glob
import yaml
import json
import great_expectations as ge
import datetime
from great_expectations.core.batch import RuntimeBatchRequest
from kedro.framework.session import KedroSession
import click


def base_ge_folder_exists():
    base_folder = os.getcwd()
    ge_folder = os.path.join(base_folder, "great_expectations")
    if os.path.exists(ge_folder) is True:
        return True
    else:
        message = """
        This command has NOT been run
        Kedro expectations wasn't initiated yet!
        Please run \'kedro expectations init\' before running this command.
        """
        print(message)
        return False


def base_ge_folder_does_NOT_exist():
    base_folder = os.getcwd()
    ge_folder = os.path.join(base_folder, "great_expectations")
    if os.path.exists(ge_folder) is False:
        return True
    else:
        message = """
        This command has NOT been run
        Kedro expectations was already initiated and is ready to use.
        If you want to reset everything related to the plugin, you
        can delete the great_expectations folder and run init again
        """
        print(message)
        return False


def location_is_kedro_root_folder():
    try:
        project_path = os.getcwd()
        KedroSession.create(project_path=project_path)
        return True
    except ModuleNotFoundError:
        print("""
        Cannot run command!
        You need to be in a kedro root folder to use Kedro Expectations!
        """)
        return False


def is_dataset_in_catalog(input, catalog):
    if input in catalog.list():
        return True
    else:
        print(
            f"The input {input} was not found at the DataCatalog.\n",
            "The following datasets are available for use:\n" 
        )
        print(*catalog.list(), sep=', ')
        return False


def dot_to_underscore(value):
    adjusted_value = str(value).replace(".", "_")
    return adjusted_value


def get_property_from_catalog_item(catalog_item, item_property):
    current_dir_path = os.getcwd()
    catalog_path = os.path.join(
        current_dir_path,
        "conf",
        "base",
        "catalog.yml"
    )
    try:
        with open(catalog_path, "r") as stream:
            catalog = yaml.safe_load(stream)
            dataset = catalog[catalog_item]
            return dataset[item_property]
    except KeyError:
        print(f"item {catalog_item} not in catalog")


def validate(adjusted_key, suite_name, validation_df):
    context = ge.get_context()
    formatted_time = datetime.datetime.now(
        datetime.timezone.utc
    ).strftime("%Y%m%dT%H%M%S")

    checkpoint_config = {
        "name": "my_missing_keys_checkpoint", #Trocar isso em certo momento
        "config_version": 1,
        "class_name": "SimpleCheckpoint",
        "validations": [
            {
                "batch_request": {
                    "datasource_name": "validation_datasource",
                    "data_connector_name": "default_runtime_data_connector_name",
                    "data_asset_name": adjusted_key,
                },
                "expectation_suite_name": suite_name,
            }
        ],
    }
    context.add_checkpoint(**checkpoint_config)

    validation_result = context.run_checkpoint(
        run_name="Kedro-Expectations-Run"+formatted_time,
        checkpoint_name="my_missing_keys_checkpoint", #Trocar isso em certo momento
        batch_request={
            "runtime_parameters": {"batch_data": validation_df},
            "batch_identifiers": {
                "default_identifier_name": "default_identifier"
            },
        },
    )
    return validation_result


def get_all_expectations(adjusted_key):
    exp_suites_path = os.path.join(
        os.getcwd(),
        "great_expectations",
        "expectations",
        adjusted_key,
    )
    all_expectations = glob.glob(
        os.path.join(
            exp_suites_path, "*.json"
        )
    )
    return all_expectations


def get_suite_name(exp_file, adjusted_key):
    parent_path, filename = os.path.split(exp_file)
    suite_name = adjusted_key + "." + filename[:-5]
    return suite_name










def create_raw_suite(adjusted_input, suite_name, expectation_suite_name):
    new_suite_path = os.path.join(
        os.getcwd(),
        "great_expectations",
        "expectations",
        adjusted_input, #Tem que criar a pasta se nao existir, pô
        suite_name+".json",
    )

    json_base = {
        "meta": {
            "great_expectations_version": "0.15.32",
        },
        "data_asset_type": None,
        "expectation_suite_name": expectation_suite_name,
        "expectations": [],
        "ge_cloud_id": None,
    }

    with open(new_suite_path, 'w') as f:
        json.dump(json_base, f, ensure_ascii=False, indent=4)


def populate_new_suite(input_data, expectation_suite_name):
    ge_context = ge.data_context.DataContext()

    batch_request = {
        "runtime_parameters": {"batch_data": input_data},
        "data_connector_name": "default_runtime_data_connector_name",
        "datasource_name": "validation_datasource",
        "data_asset_name": "asset",
        "batch_identifiers": {
            "default_identifier_name": "default_identifier"
        }
    }

    validator = ge_context.get_validator(
        batch_request=RuntimeBatchRequest(**batch_request),
        expectation_suite_name=expectation_suite_name,
    )

    exclude_column_names = []  #Add this functionality

    result = ge_context.assistants.onboarding.run(
        batch_request=batch_request,
        exclude_column_names=exclude_column_names,
    )
    validator.expectation_suite = result.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    validator.save_expectation_suite(discard_failed_expectations=False)


def choose_valid_suite_name():
    suite_name = "."
    while '.' in suite_name or ',' in suite_name or ' ' in suite_name:
        suite_name = click.prompt('', type=str)
        if '.' in suite_name or ' ' in suite_name:
            print(
                "Please choose another name for your suite.",
                "It cannot contain dots, commas or spaces"
            )
    return suite_name