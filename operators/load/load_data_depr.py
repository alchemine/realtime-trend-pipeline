import sys
sys.path.append("/opt/airflow/dags")
from realtime_trend_pipeline.common import *

import sys
from os.path import join, dirname, abspath
from pyspark.sql import SparkSession

from hdfs import InsecureClient


def load(file_path: str, db_name: str, table_name: str):
    spark = SparkSession\
        .builder\
        .appName("realtime_trend_pipeline")\
        .enableHiveSupport()\
        .getOrCreate()

    # 2. Create DB and table
    spark.sql(f"""
    CREATE DATABASE IF NOT EXISTS {db_name}
    LOCATION "hdfs://namenode:{PATH.hdfs}/{PATH.app}"
    """)

    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}.{table_name} (
            `date`  TIMESTAMP,
            rank    INT,
            title   STRING
        )
        STORED AS ORC
    """)
    # TBLPROPERTIES('skip.header.line.count' = '1')

    # 1. Load local csv to hdfs
    # df = spark.read.option("header", True).csv("file://" + file_path)
    # df = spark.createDataFrame(pd.read_csv(file_path))


    # # 4. Register DataFrame as a temporary table
    # df.createOrReplaceTempView('temp_table')
    #
    # # 5. Insert data
    # spark.sql(f"""
    #     INSERT INTO {db_name}.{table_name}
    #     SELECT * FROM temp_table
    # """)
    #
    # # 6. Show result
    # print(100*"=")
    # spark.sql(f"""
    # SELECT *
    # FROM {db_name}.{table_name}
    # """).show()
    # print(100*"=")

    spark.stop()


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Usage: <script> <file_path> <db_name> <table_name>")
        sys.exit(-1)

    load(sys.argv[1], sys.argv[2], sys.argv[3])
