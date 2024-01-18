import json
import os

import delta
from pyspark.sql import types

class IngestaoBronze:
    def __init__(
        self,
        path_full_load,
        path_incremental,
        file_format,
        table_name,
        database_name,
        id_fields,
        timestamp_field,
        partition_fields,
        read_options,
        spark,
    ):
        self.path_full_load = path_full_load
        self.path_incremental = path_incremental
        self.file_format = file_format
        self.table_name = table_name
        self.database_name = database_name
        self.id_fields = id_fields
        self.timestamp_field = timestamp_field
        self.partition_fields = partition_fields
        self.read_options = read_options
        self.spark = spark

        self.table_fullname = f"{self.database_name}.{self.table_name}"
        self.checkpoint_path = f'{self.path_incremental.rstrip("/")}_{self.table_name}_checkpoint'
        
        self.schema = None
        self.set_schema()
        self.set_query()

    def infer_schema(self):
        return self.load_full().schema

    def load_schema(self):
        schema_path = f"schema/{self.table_name}.json"
        if os.path.exists(schema_path):
            with open(schema_path, "r") as open_file:
                return types.StructType.fromJson(json.load(open_file))
        else:
            return None
        
    def save_schema(self):
        data = self.schema.jsonValue()
        with open(f'schema/{self.table_name}.json', 'w') as open_file:
            json.dump(data, open_file, indent=2)

    def set_schema(self):
        schema = self.load_schema()
        if schema is None:
            print("Inferindo schema...")
            schema = self.infer_schema()
            print("ok")
        self.schema = schema

    def load_full(self):

        reader = (self.spark
                      .read
                      .format(self.file_format)
                      .options(**self.read_options))
                  
        if self.schema != None:
            reader = reader.schema(self.schema)
                  
        df = reader.load(self.path_full_load)
        return df

    def save_full(self, df):
        writer = (
            df.coalesce(1)
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
        )

        if len(self.partition_fields) > 0:
            writer = writer.partitionBy(*self.partition_fields)

        writer.saveAsTable(self.table_fullname)

    def process_full(self):
        df = self.load_full()
        view_name_tmp = f"tb_full_{self.table_name}"
        df.createOrReplaceTempView(view_name_tmp)
        df_transform = self.transform(view_name_tmp)
        self.save_full(df_transform)

    def transform(self, table_name):
        query = self.query.format(table=table_name)
        return self.spark.sql(query)

    def read_query(self):
        path = f"etl/{self.table_name}.sql"
        if os.path.exists(path):
            with open(path, "r") as open_file:
                return open_file.read()
        return None

    def set_query(self):
        query = self.read_query()
        if query is None:
            print("Carregando query default")
            query = self.default_query()
            print("Ok.")
        self.query = query

    def default_query(self):
        base_query = """SELECT *,
                        NOW() as ingestion_at
                        FROM {table} """
        
        if len(self.id_fields) > 0:
            ids = ",".join(self.id_fields)
            window = f"""QUALIFY row_number() OVER (PARTITION BY {ids} ORDER BY {self.timestamp_field} DESC) = 1"""
        
        else:
            window = ""
        
        return base_query + window

    def load_stream(self):
        df_stream = (
            self.spark
            .readStream
            .format("cloudFiles")
            .option("cloudFiles.format", self.file_format)
            .option("cloudFiles.maxFilesPerTrigger", 10000)
            .options(**self.read_options)
            .schema(self.schema)
            .load(self.path_incremental)
        )
        return df_stream

    def save_stream(self, df_stream):
        table_delta = delta.DeltaTable.forName(self.spark, self.table_fullname)


        return (
            df_stream.writeStream.trigger(availableNow=True)
            .option("checkpointLocation", self.checkpoint_path)
            .foreachBatch(lambda df, batchID: self.upsert(df, table_delta))
        )

    def upsert(self, df, delta_table):
        df.createOrReplaceGlobalTempView(f"tb_stream_{self.table_name}")

        df_new = self.transform(table_name=f"global_temp.tb_stream_{self.table_name}")

        print(df_new.toPandas().head())

        join = " AND ".join(f"d.{i} = n.{i}" for i in self.id_fields)
        (
            delta_table.alias("d")
            .merge(df_new.alias("n"), join)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    def process_stream(self):
        df_stream = self.load_stream()
        stream = self.save_stream(df_stream)
        return stream.start()