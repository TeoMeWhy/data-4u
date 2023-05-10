# Databricks notebook source
# DBTITLE 1,Funções e Classes
import os
import json

from pyspark.sql.types import StructType
import delta

def table_exists(table, database):
    count = (spark.sql(f"show tables from {database}")
                  .filter(f"tableName = '{table}'")
                  .count())
    return count == 1

class Ingestor:

    def __init__(self, full_load_path:str, cdc_path:str, database:str, table:str, file_format:str, id_fields:[], timestamp_field:str, partiton_fields:[], options:dict):
        self.full_load_path = full_load_path
        self.cdc_path = cdc_path
        self.database = database
        self.table = table
        self.file_format = file_format
        self.id_fields = id_fields
        self.timestamp_field = timestamp_field
        self.partiton_fields = partiton_fields
        self.options = options
        self.schema = None

        self.set_schema()
        self.set_query()


    def read_folder(self, folder_path):
        reader = (spark.read.format(self.file_format).options(**self.options))

        if self.schema != None:
            reader = reader.schema(self.schema)

        df = reader.load(folder_path)
        return df


    def read_query(self):
        path = f'etl/{self.table}.sql'
        if os.path.exists(path):
            with open(path, 'r') as open_file:
                return open_file.read()


    def create_query(self):
        id_fields = ", ".join(self.id_fields)
        window = f' QUALIFY ROW_NUMBER() OVER (PARTITION BY {id_fields} ORDER BY {self.timestamp_field} DESC) = 1'
        base = 'SELECT *, NOW() AS dt_ingestao FROM {table}'
        return base + window


    def set_query(self):
        query = self.read_query()
        self.query = query if query != None else self.create_query()


    def infer_schema(self, df):
        return df.schema


    def load_schema(self):
        path = f'schemas/{self.table}.json'
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as open_file:
                json_schema = json.load(open_file)
            return StructType.fromJson(json_schema)


    def save_schema(self, schema):
        path = f'schemas/{self.table}.json'
        with open(path, 'w', encoding='utf-8') as open_file:
            json.dump(schema.jsonValue(), open_file, indent=4)


    def set_schema(self, save_schema=False):
        schema = self.load_schema()
        if schema == None:
            df = self.read_folder(self.full_load_path)
            schema = self.infer_schema(df)
        self.schema = schema
        if save_schema:
            self.save_schema(schema)


    def save_full_load(self, df):
        table_name = f'{self.database}.{self.table}'
        writer = (df.coalesce(1)
                    .write
                    .format('delta')
                    .mode('overwrite')
                    .option('overwriteSchema', 'true'))
        
        if len(self.partiton_fields) > 0:
            writer.partitionBy(*self.partiton_fields)

        writer.saveAsTable(table_name)


    def etl(self, table_placeholder):
        query = self.query.format(table=table_placeholder)
        df = spark.sql(query)
        return df


    def process_full_load(self):
        df = self.read_folder(self.full_load_path)
        df.createOrReplaceTempView(f'{self.table}_tmp_view')
        df_new = self.etl(f'{self.table}_tmp_view')
        self.save_full_load(df_new)

    def upsert(self, df_batch, delta_table):
    
        df_batch.createOrReplaceGlobalTempView(f'{self.table}_tmp_view')
        table = f'global_temp.{self.table}_tmp_view'
        
        # aqui executa o ETl para o df_batch
        df_new = self.etl(table)

        join = " AND ".join([f"d.{i} = i.{i}" for i in self.id_fields])

        (delta_table.alias("d")
                    .merge(df_new.alias("i"), join)
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute())

    def read_stream(self):
        df_stream = (spark.readStream
                          .format('cloudFiles')
                          .option('cloudFiles.format', self.file_format)
                          .options(**self.options)
                          .schema(self.schema)
                          .load(self.cdc_path))
        
        return df_stream

    def write_stream(self, df_stream):

        delta_table = delta.DeltaTable.forName(spark, f'{self.database}.{self.table}')
        
        checkpoint_path = self.cdc_path.replace(f"/{self.table}/", f'/checkoints_stream_ingestion/{self.table}/')
        
        stream = (df_stream.writeStream
                           .trigger(availableNow=True)
                           .option("checkpointLocation", checkpoint_path)
                           .foreachBatch(lambda df, batchID: self.upsert(df, delta_table)))

        return stream

    def process_stream(self):
        df_stream = self.read_stream()
        stream = self.write_stream(df_stream)
        stream.start()
        return stream


# COMMAND ----------

# DBTITLE 1,Setup
table = 'companies'
database = 'bronze.igdb'
full_load_path = f'/mnt/datalake/igdb/{table}/'
cdc_path = f'/mnt/datalake/igdb/{table}/'
file_format = 'json'
id_fields = ['id']
timestamp_field = 'updated_at'
partition_fields = []
options_reader = {"multiLine": "true"}

# COMMAND ----------

# DBTITLE 1,Intância e Execução
ingestor = Ingestor(full_load_path=full_load_path,
                    cdc_path=cdc_path,
                    database=database,
                    table=table,
                    file_format=file_format,
                    id_fields=id_fields,
                    timestamp_field=timestamp_field,
                    partiton_fields=partition_fields,
                    options=options_reader)

if not table_exists(table, database):
    print("Processando carga full-load...")
    df = ingestor.process_full_load()
    print("Ok")

print("Iniciando stream")
stream = ingestor.process_stream()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*), count(distinct id) FROM bronze.igdb.companies
# MAGIC
# MAGIC -- count(1)	count(DISTINCT id)
# MAGIC -- 327940	232162
