# Databricks notebook source

import os
import delta

class IngestaoBronze:

    def __init__(self,
                 path_full_load,
                 path_incremental,
                 file_format,
                 table_name,
                 database_name,
                 id_fields,
                 timestamp_field,
                 partition_field,
                 ):

        self.path_full_load= path_full_load
        self.path_incremental= path_incremental
        self.file_format= file_format
        self.table_name= table_name
        self.database_name= database_name
        self.id_fields= id_fields
        self.timestamp_field= timestamp_field
        self.partition_field= partition_field

        self.schema = self.infer_schema()

    def infer_schema(self):
        return self.load_full().schema

    def load_full(self):
        df = spark.read.format(self.file_format).load(self.path_full_load)
        return df

    def save_full(self, df):
        (df.coalesce(1)
           .write
           .format('delta')
           .mode('overwrite')
           .option('overwriteSchema', 'true')
           .saveAsTable(f'{self.database_name}.{self.table_name}'))

    def process_full(self):
        df = self.load_full()
        view_name_tmp = f'tb_full_{self.table_name}'
        df.createOrReplaceTempView(view_name_tmp)
        df_transform = self.transform(view_name_tmp)
        self.save_full(df_transform)

    def transform(self, table_name):
        return spark.sql(self.set_query().format(table=table_name))

    def read_query(self):
        path = f'etl/{self.table_name}.sql'
        if os.path.exists(path):
            with open(path, 'r') as open_file:
                return open_file.read()
        return None

    def set_query(self):
        return self.read_query() if self.read_query() is not None else self.default_query()

    def default_query(self):
        base_query = '''SELECT *,
                        NOW() as ingestion_at
                        FROM {table} '''
        ids = ','.join(self.id_fields)
        window = f'''QUALIFY row_number() over (partition by {ids} order by {self.timestamp_field} DESC) = 1'''
        return base_query + window

    def load_stream(self):

        df_stream = (spark.readStream
                          .format('cloudFiles')
                          .option("cloudFiles.format", self.file_format)
                          .schema(self.schema)
                          .load(self.path_incremental))
        return df_stream


    def save_stream(self, df_stream):

        table_delta = delta.DeltaTable.forName(spark, f'{self.database_name}.{self.table_name}')

        checkpoint_name = f'{self.path_incremental.rstrip("/")}_checkpoint'
        
        return (df_stream.writeStream
                         .trigger(availableNow=True)
                         .option("checkpointLocation", checkpoint_name)
                         .foreachBatch(lambda df, batchID: self.upsert(df, table_delta)))
        
    def upsert(self, df, delta_table):

        df.createOrReplaceGlobalTempView(f'tb_stream_{self.table_name}')

        df_new = self.transform(table_name=f'global_temp.tb_stream_{self.table_name}')

        join = " AND ".join(f"d.{i} = n.{i}" for i in self.id_fields)
        (delta_table.alias("d")
                    .merge(df_new.alias("n"), join)
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute())

# COMMAND ----------

ingest = IngestaoBronze(
            path_full_load = "/mnt/datalake/tabnews/contents/",
            path_incremental = "/mnt/datalake/tabnews/contents/",
            file_format = 'json',
            table_name = 'contents',
            database_name = 'bronze.tabnews',
            id_fields = ['id'],
            timestamp_field = 'updated_at',
            partition_field = [],
)

# ingest.process_full()

# COMMAND ----------

df_stream = ingest.load_stream()
stream = ingest.save_stream(df_stream)
stream_start = stream.start()
