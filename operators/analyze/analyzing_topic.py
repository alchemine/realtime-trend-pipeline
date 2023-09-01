import sys
sys.path.append("/opt/airflow/dags")
from realtime_trend_pipeline.common import *
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def get_recent_topics(table: str, output_path: str):
    # ------------------------------------------------------------
    # 1. Open session
    # ------------------------------------------------------------
    spark = SparkSession\
        .builder\
        .appName("realtime_trend_pipeline")\
        .config("hive.metastore.uris", "thrift://192.168.0.164:9083")\
        .enableHiveSupport()\
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    # ------------------------------------------------------------
    # 2. Get topics for recent 1 hour
    # ------------------------------------------------------------
    params = dict(
        n_iters_per_hour  = 6,   # Schedule interval: 10 minutes
        n_topics_per_iter = 10,  # Crawl 10 topics per one pipeline
        prev_hour         = (datetime.strptime(basename(output_path), "%Y-%m-%d_%H-%M-%S") - timedelta(hours=1)).strftime("%Y-%m-%d_%H-00-00"),
        cur_hour          = (datetime.strptime(basename(output_path), "%Y-%m-%d_%H-%M-%S")                     ).strftime("%Y-%m-%d_%H-00-00")
    )
    
    recent_topics = spark.sql(f"""
        SELECT
            rank,
            title
        FROM
            {table}
        WHERE
            hour IN (
                "{params['prev_hour']}",
                "{params['cur_hour']}"
            )
        ORDER BY
            time DESC
        LIMIT
            {params['n_iters_per_hour']*params['n_topics_per_iter']}
    """)


    # ------------------------------------------------------------
    # 3. Aggregation
    # ------------------------------------------------------------
    raw_result = recent_topics \
        .groupby('title')\
        .agg(
            F.count('title').alias('count_title'),
            F.avg('rank').alias('avg_rank')
        )
    raw_result.createOrReplaceTempView("raw_result")
    
    result = spark.sql(f"""
        SELECT
            title AS `최근 1시간 인기 검색어`,
            ROUND(count_title/{params['n_iters_per_hour']}*100, 1) AS `출현비율(%)`,
            ROUND(avg_rank, 1) AS `평균순위`
        FROM
            raw_result
        ORDER BY
            `출현비율(%)` DESC,
            `평균순위` ASC
        LIMIT
            {params['n_topics_per_iter']}
    """)


    # ------------------------------------------------------------
    # 4. Output
    # ------------------------------------------------------------
    result.coalesce(1).write.csv(output_path, header=True)
    result.show()
    
    spark.stop()


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: <script> <table> <output_path>")
        sys.exit(-1)

    get_recent_topics(sys.argv[1], sys.argv[2])
