import click


@click.group(name="Kedro-Expectations")
def commands():
    """Kedro Expectations Command collection"""


@commands.group()
def expectations():
    """Run Kedro Expectations Commands"""


from .init_ge import init
from .create_suite import create_suite

expectations.add_command(init)
expectations.add_command(create_suite)

def main():
    commands()


if __name__ == "__main__":
    main()
