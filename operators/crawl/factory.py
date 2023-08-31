from realtime_trend_pipeline.common import *

from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator


def crawler_factory(image_type, file_path):
    # --------------------------------------------------
    # 1. Prepare parameters
    # --------------------------------------------------
    common_params = dict(
        task_id='crawl',
        container_name='realtime_trend_pipeline-crawl',
        api_version='auto',
        auto_remove='force',
        mounts=[
            Mount(source=os.getenv('HDFS_DIR'), target=PATH.hdfs, type='bind'),  # source: host, target: container
            Mount(source=os.getenv('DFS_DIR'),  target=PATH.dfs, type='bind'),   # source: host, target: container
            Mount(source=os.getenv('DAGS_DIR'), target=PATH.dags, type='bind'),  # source: host, target: container
        ],
        docker_url="tcp://docker-proxy:2375",
        mount_tmp_dir=False,
    )

    cmd_install_python = """
        sudo apt update && apt install -yqq python3.8 python3.8-distutils
        curl https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py
        python3 /tmp/get-pip.py
        rm /tmp/get-pip.py
    """
    cmd_install_python_deps = """
        python3 -m pip install pandas selenium hdfs
    """
    cmd_execute_python = f"""
        cd {PATH.dags}
        python3 -m realtime_trend_pipeline.operators.crawl.topic_crawling {file_path}
    """


    # --------------------------------------------------
    # 2. Get operator
    #   image_type = unofficial: python is installed
    #   image_type =   official: python is not installed
    # --------------------------------------------------
    if image_type == 'non-official':
        params = dict(
            image='joyzoursky/python-chromedriver:3.8-selenium',
            command=f"""
                bash -c "
                {cmd_install_python_deps}
                {cmd_execute_python}
                "
            """,
            **common_params
        )
    elif image_type == 'official':
        params = dict(
            image='selenium/standalone-chrome:90.0.4430.85-chromedriver-90.0.4430.24',
            command=f"""
                bash -c "
                {cmd_install_python}
                {cmd_install_python_deps}
                {cmd_execute_python}    
                "
            """,
            **common_params
        )
    else:
        raise ValueError(image_type)

    return DockerOperator(**params)
