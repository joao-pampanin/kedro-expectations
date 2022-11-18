import click


@click.group(name="Kedro-Expectations")
def commands():
    """Kedro Expectations Command collection"""


@commands.group()
def expectations():
    """Run Kedro Expectations Commands"""


from .generate_datasources import generate_generic_datasource

expectations.add_command(generate_generic_datasource)


def main():
    commands()


if __name__ == "__main__":
    main()
