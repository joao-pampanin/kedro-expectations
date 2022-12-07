# kedro-expectations
A tool to better integrate Kedro and Great Expectations

## Introduction

Kedro Expectaions is a tool designed to make the use of Great Expectations within ProjetaAi projects easier. It is composed of a couple commands and a hook, allowing the user create suites and run validations based on the DataCatalog and using directly the Kedro input as it's called by the normal pipeline

## Features

- ⏳ Initialization of GE without having to worry about datasources
- 🎯 Creation of GE suites automatically, using the Data Assistant
- 🚀 Running validations within the Kedro pipeline

## Installation

For now, the plugin can only be installed through this github repo, but soon it will be available at PyPI

## Usage

### CLI Usage

The first step to use the plugin is running an init command. This command will create the base GE folder and create the only datasource the plugin needs

```bash
kedro expectations init
```

After the init command the plugin is ready to create expectation suites. It is possible to create expectation suites for Non-spark dataframe objects (there is no need to worry about file extension since Kedro Expectations gets all it needs from the Kedro input) and ProjetaAi Partitioned datasets

Within partitioned datasets, it is possible to create generic expectations, meaning all the partitions will use that expectation, or specific expectations, meaning only the specified partition will use the generated expectation

Run the following command to create expectations suites:

```bash
kedro expectations create-suite
```

### Hook Usage

In order to enable the hook capabilities you only need to call it in the settings.py file inside your kedro project

(inside src/your_project_name/settings.py)
```bash
from kedro_expectations import KedroExpectationsHooks

HOOKS = (KedroExpectationsHooks(fail_fast=False),)
```

### Fail Fast

fail_fast is a parameter added to allow a great expectations validation failure to break the pipeline run. It is useful when you want your pipeline to keep running only if GE doesn't find any incoherences at any validation

Its default value is "False", and to change it the only step necessary is to change the parameter value within your hook usage

```bash
HOOKS = (KedroExpectationsHooks(fail_fast=True),)
```

