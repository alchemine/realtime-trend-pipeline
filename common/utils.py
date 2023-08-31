from realtime_trend_pipeline.common.config import *
from realtime_trend_pipeline.common.env import *


class PATH:
    app  = 'realtime_trend_pipeline'
    root = '/opt/airflow'
    dags = join(root, 'dags')
    src  = join(dags, app)
    operators = join(src, 'operators')
    hdfs = join('/hdfs', app)
    dfs  = join('/dfs', app)
