from realtime_trend_pipeline.common import *
from realtime_trend_pipeline.operators.crawl.factory import crawler_factory
from realtime_trend_pipeline.operators.postprocess.postprocess import process_output_path_to_message
from realtime_trend_pipeline.operators.request_message.request_message import producer as request_message_producer

from datetime import datetime, timedelta

from airflow import DAG
# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
# from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
# from airflow.exceptions import AirflowFailException
from kafka import KafkaProducer


PARAMS = dict(
    db=PATH.app,
    csv_table=f"{PATH.app}.topic_tmp",
    orc_table=f"{PATH.app}.topic",
    db_path=join(PATH.hdfs, f"{PATH.app}.db"),
    topic_request_message='recent_topics',
    # topic_request_message_monitor='recent_topics_monitor',
    kafka_bootstrap_servers=['kafka-server:19092']
)


def on_failure(context):
    # TODO: add new topic
    producer = KafkaProducer(bootstrap_servers=PARAMS['kafka_bootstrap_servers'])
    producer.send(
        PARAMS['topic_request_message'],
        json.dumps({'text': 'Failure occurs', 'output_path': None})
    )


default_args = {
    'owner': 'alchemine',
    'start_date': datetime(2023, 9, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure
}


# def consumer(msg, output_path: str):
#     msg = json.loads(msg.decode('utf-8'))
#     if msg.value['output_path']:
#         print("[Success] Sending message")
#     else:
#         print("[Failure] Sending message")
#         raise AirflowFailException


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
    cur_time  = "{{ execution_date.strftime('%Y-%m-%d_%H-%M-%S') }}"
    file_path = join(PATH.input, "topics", f"{cur_time}.csv")
    PARAMS['local_csv_path'] = file_path
    PARAMS['output_path']    = join(PATH.output, "recent_topics", cur_time)  # directory


    # ------------------------------------------------------------
    # 1. Crawl data to local storage
    # ------------------------------------------------------------
    crawl = crawler_factory(
        image_type='unofficial',  # 'unofficial': python installed, 'official': python uninstalled
        file_path=PARAMS['local_csv_path']
    )


    # ------------------------------------------------------------
    # 2. Load local csv data to hdfs
    # ------------------------------------------------------------
    load = HiveOperator(
        task_id='load',
        hql=f"""
            -- 1. Create database
            -- DROP DATABASE {PARAMS["db"]} CASCADE;
            CREATE DATABASE IF NOT EXISTS {PARAMS["db"]}
            LOCATION "{PARAMS['db_path']}";

            
            -- 2. Create temporary CSV table
            CREATE TEMPORARY TABLE {PARAMS["csv_table"]} (
                `time` TIMESTAMP, rank INT, title STRING
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            LINES TERMINATED BY '\n'
            STORED AS TEXTFILE
            TBLPROPERTIES('skip.header.line.count'='1');
            
            LOAD DATA LOCAL INPATH "{PARAMS['local_csv_path']}"
            OVERWRITE INTO TABLE {PARAMS["csv_table"]};


            -- 3. Create ORC table
            CREATE EXTERNAL TABLE IF NOT EXISTS {PARAMS["orc_table"]} (
                `time` TIMESTAMP, rank INT, title STRING
            )
            PARTITIONED BY (hour STRING)
            STORED AS ORC;

            INSERT INTO TABLE {PARAMS["orc_table"]} PARTITION (hour="{{{{ execution_date.strftime('%Y-%m-%d_%H-00-00') }}}}")
            SELECT * FROM {PARAMS["csv_table"]};
            
            
            -- 4. Show table
            SELECT * FROM {PARAMS["orc_table"]};
        """,
        hive_cli_conn_id='hive_cli_conn'
    )

    
    # ------------------------------------------------------------
    # 3. Analyze data from hdfs table
    # ------------------------------------------------------------
    analyze = SparkSubmitOperator(
        task_id='analyze',
        application=join(PATH.operators, 'analyze/analyzing_topic.py'),
        application_args=[PARAMS['orc_table'], PARAMS['output_path']],
        conn_id='spark_conn'
    )


    # ------------------------------------------------------------
    # 4. Process analysis result to message
    # ------------------------------------------------------------
    postprocess = PythonOperator(
        task_id='postprocess',
        python_callable=process_output_path_to_message,
        op_kwargs={'output_path': PARAMS['output_path']}
    )

 
    # ------------------------------------------------------------
    # 5. Send message to Kakao messaging server
    # ------------------------------------------------------------
    request_message = ProduceToTopicOperator(
        task_id='request_message',
        kafka_config_id='kafka_default',
        topic=PARAMS['topic_request_message'],
        producer_function=request_message_producer,
        producer_function_kwargs=dict(
            text="{{ ti.xcom_pull(task_ids='postprocess') }}",
            output_path=PARAMS['output_path']
        )
        # poll_timeout=10
    )


    # ------------------------------------------------------------
    # 5. Receive result message from Kakao messaging server
    # ------------------------------------------------------------
    # result_monitor = ConsumeFromTopicOperator(
    #     task_id='result_monitor',
    #     kafka_config_id='kafka_default',
    #     topics=[PARAMS['topic_request_message_monitor']],
    #     apply_function=consumer,
    #     apply_function_args=[PARAMS['output_path']],
    #     poll_timeout=120,
    #     max_messages=1,
    #     # max_batch_size=20
    # )


    # ------------------------------------------------------------
    # 6. Finish
    # ------------------------------------------------------------
    finish = DummyOperator(
        task_id='finish'
    )

    crawl >> load >> analyze >> postprocess >> request_message >> finish
