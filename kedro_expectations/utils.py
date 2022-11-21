def get_execution_engine_class(dataset, fileextension):
    # Warning - Não existe suporte pra .pickle e .pq
    pandas_supported_types = [ ".csv", ".tsv", ".xls", ".xlsx", ".parquet", ".parq", ".pqt", ".json", ".pkl", ".feather", ".csv.gz", ".tsv.gz", ".sas7bdat", ".xpt"]
    spark_supported_types = [ ".csv", ".tsv", ".parquet", ".parq", ".pqt"]

    if (not str(dataset['type']).startswith('spark')) and (fileextension in pandas_supported_types):
        execution_engine_class = "PandasExecutionEngine"
        execution_engine_aux_name = "pandas"
        return execution_engine_class, execution_engine_aux_name
    else:
        return None, None