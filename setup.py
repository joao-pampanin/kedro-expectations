from setuptools import find_packages, setup

# with open("README.md", "r") as fh:
#     long_description = fh.read()

setup(
    name="kedro-expectations",
    version="0.1.0",
    url="https://github.com/tamsanh/kedro-great.git", #Mudar
    author="Joao Gabriel Pampanin de Abreu",
    author_email="jgpampanin@protonmail.com", # Criar e-mail de prog
    description="Integrate Kedro and Great Expectations",
    long_description="Long Integrate Kedro and Great Expectations",
    long_description_content_type="text/markdown",
    packages=["kedro_expectations"],
    zip_safe=False,
    include_package_data=True,
    license="MIT",
    install_requires=[
        "kedro==0.18.2",
        "kedro[pandas]==0.18.2",
        "kedro[spark]==0.18.2",
        "great_expectations==0.15.32",
        "pyspark",
        "pandas",
    ],
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.9",
    ],
    entry_points={
        "kedro.global_commands": ["kedro-expectations = kedro_expectations:commands"]
    }
)
