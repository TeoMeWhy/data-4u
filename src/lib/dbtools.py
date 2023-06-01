
def table_exists(spark, database, table):
    count = (spark.sql(f"SHOW TABLES FROM {database}")
                  .filter(f"tableName = '{table}'")
                  .count())
    return count == 1


def import_query(path, **kwargs):
    with open(path, 'r',**kwargs) as open_file:
        return open_file.read()