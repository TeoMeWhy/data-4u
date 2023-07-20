def table_exists(spark, database, table):
    count = (spark.sql(f"SHOW TABLES FROM {database}")
                  .filter(f"tableName = '{table}'")
                  .count())
    return count == 1


def import_query(path, **kwargs):
    with open(path, 'r',**kwargs) as open_file:
        return open_file.read()
    

def get_schemas_from_catalog(spark, catalog, ignored=[]):
    ignored = ",".join([f"'{i}'" for i in ignored])
    databases = (spark.sql(f"SHOW DATABASES FROM {catalog}")
                      .filter(f"databaseName not in ({ignored})") 
                      .toPandas()["databaseName"]
                      .tolist())
    return databases


def get_tables_from_database(spark, database):
    tables = (spark.sql(f"SHOW TABLES FROM {database}")
                   .toPandas()['tableName']
                   .tolist())
    return tables