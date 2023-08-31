from realtime_trend_pipeline.common import *
from realtime_trend_pipeline.operators.crawl.factory import crawler_factory

from datetime import datetime

from airflow import DAG

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
# from airflow.operators.hive_operator import HiveOperator
# from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'alchemine',
    'start_date': datetime(2023, 8, 31)
}


with DAG(
    dag_id='realtime_trend_pipeline',
    # schedule='*/5 * * * *',
    schedule='* * 5 * *',
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
    file_path = join(PATH.dfs, 'csv/{{ ts_nodash }}.csv')
    crawl = crawler_factory(
        image_type='unofficial',  # 'unofficial': python installed, 'official': python uninstalled
        file_path=file_path
    )


    # ------------------------------------------------------------
    # 2. Load local csv data to hdfs(/hdfs/realtime_trend_pipeline/csv/*.csv)
    # ------------------------------------------------------------


    # load = SparkSubmitOperator(
    #     task_id='load',
    #     application=join(PATH.operators, 'load/load_to_hive.py'),
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

    # crawl >> load

