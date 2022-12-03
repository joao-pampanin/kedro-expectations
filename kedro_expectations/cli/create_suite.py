import click
import os
from typing import Any, Dict, cast, Callable
import json
import pandas as pd
import great_expectations as ge
from kedro.framework.session import KedroSession
from great_expectations.core.batch import RuntimeBatchRequest
from kedro_expectations.utils import dot_to_underscore, create_raw_suite, populate_new_suite, base_ge_folder_exists


@click.command()
def create_suite() -> None:
    base_ge_folder_exists()

    option = 0
    click.echo('Type 1 if you want to create a suite for a \'normal\' dataset')
    click.echo('Type 2 if you want to create a suite for a Partitioned dataset')
    while (option != 1) and (option != 2):
        option = click.prompt('', type=int)

    if option == 1:
        click.echo('Type the dataset name as it is on the DataCatalog')
        input = click.prompt('', type=str)

        click.echo('Type the desired name for the expectation suite')
        suite_name = click.prompt('', type=str)

        project_path = os.getcwd()
        with KedroSession.create(project_path=project_path) as session:
            kedro_context = session.load_context()
            catalog = kedro_context.catalog
            if input not in catalog.list():
                print(f"Input name {input} not found inside the Kedro catalog")
            else:
                adjusted_input = dot_to_underscore(input)
                expectation_suite_name = adjusted_input+"."+suite_name

                create_raw_suite(adjusted_input, suite_name, expectation_suite_name)
                input_data = catalog.load(input)
                populate_new_suite(input_data, expectation_suite_name)

                print("\nFor more information about how to edit the expectations suite, access: https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/overview/\n")

    elif option == 2:
        option = 0
        click.echo('Type 1 if you want to create a generic expectation')
        click.echo('Type 2 if you want to create an specific expectation')
        while (option != 1) and (option != 2):
            option = click.prompt('', type=int)

        if option == 1:
            click.echo('Type the dataset name as it is on the DataCatalog')
            input = click.prompt('', type=str)

            click.echo('Type the desired name for the expectation suite')
            suite_name = click.prompt('', type=str)

            project_path = os.getcwd()
            with KedroSession.create(project_path=project_path) as session:
                kedro_context = session.load_context()
                catalog = kedro_context.catalog
                if input not in catalog.list():
                    print(f"Input name {input} not found inside the Kedro catalog")
                else:
                    adjusted_input = dot_to_underscore(input)
                    expectation_suite_name = adjusted_input+"."+suite_name

                    create_raw_suite(adjusted_input, suite_name, expectation_suite_name)
                    input_data = catalog.load(input)
                    partitions = cast(Dict[str, Callable], input_data)
                    validation_df = pd.DataFrame()

                    for casted_key, casted_value in partitions.items():
                        validation_df = pd.concat([validation_df, casted_value()], ignore_index=True, sort=False)
                        if len(validation_df.index) >= 5000:
                            break

                    populate_new_suite(validation_df, expectation_suite_name)
        elif option == 2:
            click.echo('Type the dataset name as it is on the DataCatalog')
            input = click.prompt('', type=str)

            click.echo('Type the specific partition name you want to create an Expectation Suite for')
            desired_part = click.prompt('', type=str)

            click.echo('Type the desired name for the expectation suite')
            suite_name = click.prompt('', type=str)

            project_path = os.getcwd()
            with KedroSession.create(project_path=project_path) as session:
                kedro_context = session.load_context()
                catalog = kedro_context.catalog
                if input not in catalog.list():
                    print(f"Input name {input} not found inside the Kedro catalog")
                else:
                    adjusted_input_pt1 = dot_to_underscore(input)
                    adjusted_input_pt2 = dot_to_underscore(desired_part)
                    expectation_suite_name = adjusted_input_pt1 + "." + adjusted_input_pt2 + "." + suite_name
                    adjusted_input = os.path.join(adjusted_input_pt1, adjusted_input_pt2)

                    create_raw_suite(adjusted_input, suite_name, expectation_suite_name)
                    input_data = catalog.load(input)
                    partitions = cast(Dict[str, Callable], input_data)
                    validation_df = partitions[desired_part]()
                    print(validation_df.shape)

                    populate_new_suite(validation_df, expectation_suite_name)
