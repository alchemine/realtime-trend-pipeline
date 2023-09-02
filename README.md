# realtime_trend_pipeline
실시간 검색어를 크롤링하여 hive metastore에 적재, 분석 후 분석 결과를 카카오톡 메시지로 전송하는 데이터 파이프라인

Workflow: [realtime_trend_pipeline.py](https://github.com/alchemine/realtime_trend_pipeline/blob/main/realtime_trend_pipeline.py)

![workflow](/assets/image-0.png)


## 1. Configuration
- OS: Ubuntu22.04
- Docker cluster: [alchemine/hadoop-docker-cluster](https://github.com/alchemine/hadoop-docker-cluster)
- Schedule interval: 10분 (`'*/10 * * * *'`)
- Data source: [시그널 실시간 검색어](http://signal.bz)


## 2. Airflow Workflow
### 1) Crawl
[시그널 실시간 검색어](http://signal.bz)로부터 실시간 검색어를 크롤링하고 csv format으로 local에 저장
- `airflow.providers.docker.operators.docker.DockerOperator`
- [`selenium/standalone-chrome:90.0.4430.85-chromedriver-90.0.4430.24`](https://hub.docker.com/layers/selenium/standalone-chrome/90.0.4430.85-chromedriver-90.0.4430.24-grid-4.0.0-beta-3-20210426/images/sha256-1532a6d76064edc555045aa790918fbbca972426099e0559ee4eef138dd0db62?context=explore) \

![site sample](/assets/image-1.png)
![csv sample](/assets/image-2.png)

### 2) Load
Local csv file을 hive metastore 내 external partitioned orc table에 적재
- `airflow.operators.hive_operator.HiveOperator`

![partition sample](/assets/image-3.png)
![orc schema](/assets/image-4.png)
![orc sample](/assets/image-4-1.png)


### 3) Analyze
최근 1시간(기준: `execution_date`) 동안 저장된 인기 검색어들을 읽어와 각 검색어에 대하여, 검색어 순위 내 출현비율과 평균순위를 계산한 결과를 local csv file로 저장
- `airflow.providers.apache.spark.operators.spark_submit.SparkSubmitOperator`

![output sample](/assets/image-5.png)


### 4) Request message
분석 결과를 카카오톡 메시지를 보내주는 서버에 json 형태로 publish
- `airflow.providers.apache.kafka.operators.produce.ProduceToTopicOperator`
- Kakao message server: []()

![message sample](/assets/image-6.png)


### 5) Finish
모든 작업이 정상종료되었다는 것을 확인
- `from airflow.operators.dummy_operator.DummyOperator`

![message sample](/assets/image-7.png)


<!-- # 2. Considerations
### 2.1 Kafka
1. 본 프로젝트에서 Kafka는 사실상 필요하지 않으나 응용을 위해 추가하였음.
2. 데이터를 크롤링하는 시점을 내가 임의로 정할 수 없거나(event), 데이터를 처리하는 과정이 무거워 분산처리가 필요하게 될 경우 Kafka consumer group을 사용하는 것이 의미가 있을 것이다.

### 2.2 HDFS
1. Python package [hdfs](https://pypi.org/project/hdfs/)를 이용하여 간단하게 데이터를 HDFS로 적재할 수 있었음
2. 총 4대의 docker instance를 docker-compose로 생성하여 hadoop system을 구성
  1. 1대의 namenode, 3대의 datanode (하나는 secondary namenode, replication factor: 3)
  2. namenode는 Kafka 및 zookeeper server master 이기도 하다
  3. 서버 구성이 해당 프로젝트에서 가장 시간이 오래 걸리고 힘들었던 작업이었다. 특히, WSL 위의 docker를 올려 작업하다보니 더욱 어려웠다.
  4. [https://github.com/alchemine/realtime-trend-pipeline/blob/main/scripts/docker-compose.yml](https://github.com/alchemine/realtime-trend-pipeline/blob/main/scripts/docker-compose.yml)
  
### 2.3 Hive
1. 외부에서 1시간을 단위로 하는 파티션 이름을 주입하고(`-d date_hour=...`) hql script를 실행하여 partitioning 테이블을 생성
2. ORC format으로 만들고 싶었는데 MapReduce 설정 문제로 데이터 주입이(`LOAD DATA INPATH`) 작동하지 않아 CSV table로 생성

### 2.4 Spark
1. 생성한 테이블로부터 최근 2개의 파티션(최근 2시간) 데이터를 읽어와 각 검색어(`title`)의 빈도수(`COUNT(title)`)와 평균 순위(`AVG(rank)`)를 계산하고 최종 결과물로 CSV 파일을 출력
2. scala로 스크립트를 짜긴했는데 보통은 spark를 실행하기 위해 jar file을 생성하는데 그렇게까진 하고 싶지 않아 간단히 `cat` 명령어를 이용하여 spark로 실행시켰다. \
   일반적으로 method를 사용할 때 한 줄 씩 띄우게 되는데 이렇게 사용하면 여러 줄을 하나로 인식을 못하여 하나의 라인에 명령어들을 전부 떄려박았다.
   ```
   $ cat get_popular_topic.scala | spark-shell
   ```

### 2.5 Kakao message
1. 일반적으로 Slack message로 결과를 전송하는 편이지만, 이번에는 카카오톡을 이용하여 결과를 전송하였다.
2. 데이터 분석 결과를 카카오톡 메시지로 적절하게 정제된 텍스트를 kafka를 통해 기존에 동작하고 있던 server에 보내면 server가 카카오톡 메시지를 보내준다.


### 2.6 Airflow
1. Workflow(`initialize >> extract_load_topic >> load_topic_hive >> analyze_topic >> send_message`)를 관리.
2. `BashOperator`와 `PythonOperator`를 주로 사용하여 각 task를 구현.
3. Timezone이 UTC라 변경해줄 필요가 있다.
4. 가능하면 `poetry`로 생성한 가상환경에 모든 패키지를 관리하려고 했으나, 무슨 이유 떄문인지 시스템 인터프리터(`/usr/bin/python`)를 사용하지 않으면 `airflow webserver`와 `scheduler`가 제대로 동작하지 않았다.
5. Dag script는 `~/airflow/dags`에 있지만 각 task의 세부구현은 `realtime-trend-pipeline` package에 있어 다음과 같이 맨 첫 줄에 직접 경로를 추가하였는데 좀 더 좋은 방법이 있을 것 같다.
   ```
   import sys
   sys.path.append("/workspace/project/realtime-trend-pipeline")
   ...
   ``` -->
