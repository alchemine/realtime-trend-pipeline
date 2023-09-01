from realtime_trend_pipeline.common import *
from realtime_trend_pipeline.operators.crawl.factory import crawler_factory

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
# from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.hive_operator import HiveOperator


default_args = {
    'owner': 'alchemine',
    'start_date': datetime(2023, 9, 2)
}


with DAG(
    dag_id='realtime_trend_pipeline',
    schedule='*/10 * * * *',
    # schedule='@once',
    default_args=default_args,
    tags=['realtime-trend'],
    catchup=False
) as dag:

    # Echo IP address (airflow-worker)
    # check_ip = BashOperator(
    #     task_id='check_ip',
    #     bash_command="""
    #        echo "---------------------------------------------------------"
    #        echo "[Current working node]"
    #        echo "Hostname: $(hostname)"
    #        echo "IP      : $(ifconfig eth0 | grep 'inet ' | awk '{print $2}')"
    #        echo "---------------------------------------------------------"
    #     """
    # )

    
    # ------------------------------------------------------------
    # Initialize parameters
    # ------------------------------------------------------------
    file_path = join(PATH.input, "topics", "{{ execution_date.strftime('%Y-%m-%d_%H-%M-%S') }}.csv")
    params = dict(
        db=PATH.app,
        csv_table=f"{PATH.app}.topic_tmp",
        orc_table=f"{PATH.app}.topic",
        db_path=join(PATH.hdfs, f"{PATH.app}.db"),
        local_csv_path=file_path,
        output_path=join(PATH.output, "recent_topics", "{{ execution_date.strftime('%Y-%m-%d_%H-%M-%S') }}")  # directory
    )


    # ------------------------------------------------------------
    # 1. Crawl data to local storage
    # ------------------------------------------------------------
    crawl = crawler_factory(
        image_type='unofficial',  # 'unofficial': python installed, 'official': python uninstalled
        file_path=params['local_csv_path']
    )


    # ------------------------------------------------------------
    # 2. Load local csv data to hdfs
    # ------------------------------------------------------------
    load = HiveOperator(
        task_id='load',
        hql=f"""
            -- 1. Create database
            -- DROP DATABASE {params["db"]} CASCADE;
            CREATE DATABASE IF NOT EXISTS {params["db"]}
            LOCATION "{params['db_path']}";

            
            -- 2. Create temporary CSV table
            CREATE TEMPORARY TABLE {params["csv_table"]} (
                `time` TIMESTAMP, rank INT, title STRING
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            LINES TERMINATED BY '\n'
            STORED AS TEXTFILE
            TBLPROPERTIES('skip.header.line.count'='1');
            
            LOAD DATA LOCAL INPATH "{params['local_csv_path']}"
            OVERWRITE INTO TABLE {params["csv_table"]};


            -- 3. Create ORC table
            CREATE EXTERNAL TABLE IF NOT EXISTS {params["orc_table"]} (
                `time` TIMESTAMP, rank INT, title STRING
            )
            PARTITIONED BY (hour STRING)
            STORED AS ORC;

            INSERT INTO TABLE {params["orc_table"]} PARTITION (hour="{{{{ execution_date.strftime('%Y-%m-%d_%H-00-00') }}}}")
            SELECT * FROM {params["csv_table"]};
            
            
            -- 4. Show table
            SELECT * FROM {params["orc_table"]};
        """,
        hive_cli_conn_id='hive_cli_conn',
        # run_as_owner=True
    )

    
    # ------------------------------------------------------------
    # 3. Analyze data from hdfs table
    # ------------------------------------------------------------
    analyze = SparkSubmitOperator(
        task_id='analyze',
        application=join(PATH.operators, 'analyze/analyzing_topic.py'),
        application_args=[params['orc_table'], params['output_path']],
        conn_id='spark_conn'
    )


    # send_message = PythonOperator(
    #     task_id='send_message',
    #     python_callable=send_message
    # )
    
    crawl >> load >> analyze
