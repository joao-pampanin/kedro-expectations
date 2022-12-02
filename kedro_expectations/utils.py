import os
import glob
import yaml
import json
import great_expectations as ge
import datetime
from great_expectations.core.batch import RuntimeBatchRequest

def get_execution_engine_class(dataset, fileextension):
    # Warning - Não existe suporte pra .pickle e .pq
    pandas_supported_types = [ ".csv", ".tsv", ".xls", ".xlsx", ".parquet", ".parq", ".pqt", ".json", ".pkl", ".feather", ".csv.gz", ".tsv.gz", ".sas7bdat", ".xpt"]
    # spark_supported_types = [ ".csv", ".tsv", ".parquet", ".parq", ".pqt"]

    if (not str(dataset['type']).startswith('spark')) and (fileextension in pandas_supported_types):
        execution_engine_class = "PandasExecutionEngine"
        execution_engine_aux_name = "pandas"
        return execution_engine_class, execution_engine_aux_name
    else:
        return None, None


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
    print("\n",exp_suites_path,"\n")
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
