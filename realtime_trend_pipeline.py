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
    'start_date': datetime(2023, 8, 31)
}


with DAG(
    dag_id='realtime_trend_pipeline',
    schedule='*/5 * * * *',
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
    # 1. Crawl data to local storage(/dfs/realtime_trend_pipeline/csv/*.csv)
    # ------------------------------------------------------------
    file_path = join(PATH.dfs, PATH.app, "{{ execution_date.strftime('%Y-%m-%d_%H-%M-%S') }}.csv")
    crawl = crawler_factory(
        image_type='unofficial',  # 'unofficial': python installed, 'official': python uninstalled
        file_path=file_path
    )


    # ------------------------------------------------------------
    # 2. Load local csv data to hdfs(/hdfs/realtime_trend_pipeline/csv/*.csv)
    # ------------------------------------------------------------
    params_hive_common = dict(
        hive_cli_conn_id='hive_cli_conn',
        # run_as_owner=True
    )
    params = dict(
        db=PATH.app,
        csv_table=f"{PATH.app}.topic_csv",
        orc_table=f"{PATH.app}.topic_orc",
        db_path=join(PATH.hdfs, f"{PATH.app}.db"),
        local_csv_path=file_path,
        local_csv_dir_path=dirname(file_path)
    )

    create_db = HiveOperator(
        task_id='create_db',
        hql=f"""
--             DROP DATABASE {params["db"]} CASCADE;
            CREATE DATABASE IF NOT EXISTS {params["db"]}
            LOCATION "{params['db_path']}";
        """,
        **params_hive_common
    )

    load = HiveOperator(
        task_id='load',
        hql=f"""
            -- 1. Create temporary CSV table
            CREATE TEMPORARY TABLE {params["csv_table"]} (
                `time` TIMESTAMP, rank INT, title STRING
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            LINES TERMINATED BY '\n'
            STORED AS TEXTFILE
            TBLPROPERTIES('skip.header.line.count'='1');
            
            LOAD DATA LOCAL INPATH "{params['local_csv_dir_path']}"
            OVERWRITE INTO TABLE {params["csv_table"]};


            -- 2. Create ORC table
            CREATE EXTERNAL TABLE IF NOT EXISTS {params["orc_table"]} (
                `time` TIMESTAMP, rank INT, title STRING
            )
            PARTITIONED BY (hour STRING)
            STORED AS ORC;

            INSERT INTO TABLE {params["orc_table"]} PARTITION (hour="{{{{ execution_date.strftime('%Y-%m-%d_%H-00-00') }}}}")
            SELECT * FROM {params["csv_table"]};
            
            
            -- 3. Show table
            SELECT * FROM {params["orc_table"]};
        """,
        **params_hive_common
    )


    # load = SparkSubmitOperator(
    #     task_id='load',
    #     application=join(PATH.operators, 'load/load_data_depr.py'),
    #     application_args=[file_path, 'trend', 'topic'],
    #     conn_id='spark_cluster'
    # )


    # initialize = BashOperator(
    #     task_id='initialize',
    #     bash_command=f"bash {join(PATH.scripts, 'initialize.sh')} "
    # )
    # extract_load_topic = PythonOperator(
    #     task_id='extract_load_topic',
    #     python_callable=crawl
    # )
    # load_topic_hive = BashOperator(
    #     task_id='load_topic_hive',
    #     bash_command=f"bash {join(PATH.scripts, 'load_topic_hive.sh')} "
    # )
    # analyze_topic = BashOperator(
    #     task_id='analyze_topic',
    #     bash_command=f"cat {join(PATH.scripts, 'get_popular_topic.scala')} | spark-shell"
    # )
    # send_message = PythonOperator(
    #     task_id='send_message',
    #     python_callable=send_message
    # )
    #
    # initialize >> extract_load_topic >> load_topic_hive >> analyze_topic >> send_message

    # crawl >> create_db >> create_table_csv >> load_csv
    # crawl >> create_db >> create_csv_table >> load_csv >> show_csv_table >> create_orc_table >> load_orc >> show_orc_table
    crawl >> create_db >> load
