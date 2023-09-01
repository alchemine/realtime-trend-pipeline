from realtime_trend_pipeline.common.config import *
from realtime_trend_pipeline.common.env import *


class PATH:
    hdfs = '/hdfs'
    dfs  = '/dfs'
    app  = 'realtime_trend_pipeline'

    dags = join('/opt/airflow', 'dags')
    src  = join(dags, app)
    operators = join(src, 'operators')
