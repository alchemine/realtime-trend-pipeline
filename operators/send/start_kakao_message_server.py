from os.path import join, dirname, basename, abspath
from time import sleep
import json
from glob import glob
from datetime import datetime

import pandas as pd
from PyKakao import Message
from kafka import KafkaProducer, KafkaConsumer


def get_api():
    with open("service_key.ini") as f:  # REST API 키
        service_key = f.read()
    api = Message(service_key=service_key)
    
    print(api.get_url_for_generating_code())
    sleep(20)
    with open("kakao_auth.ini") as f:
        url = f.read()

    access_token = api.get_access_token_by_redirected_url(url)
    api.set_access_token(access_token)
    return api


def process_text(output_path: str):
    input_format  = "%Y-%m-%d_%H-%M-%S"
    output_format = "%y/%m/%d %H:%M:%S"
    
    time     = datetime.strptime(basename(output_path), input_format).strftime(output_format)
    csv_path = glob(f"{output_path}/*.csv")[0]  # *.csv is unique
    df       = pd.read_csv(csv_path)

    msg  =  "[최근 1시간 인기 검색어] \n"
    msg += f"기준시간: {time} \n"
    msg += '\n'.join(f"{rank:>2}.  {title}" for rank, title in enumerate(df.iloc[:, 0], start=1))
    return msg


def start_server(api):
    # producer = KafkaProducer(bootstrap_servers=['kafka-server:19092'])
    consumer = KafkaConsumer(
        'recent_topics',
        bootstrap_servers=['kafka-server:19092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for msg in consumer:
        output_path = msg.value['output_path']
        try:  # success
            api.send_text(text=process_text(output_path), link={}, button_title="바로 확인")
            msg_result = {output_path: True}
        except:  # failure
            msg_result = {output_path: False}
        # producer.send('recent_topics_monitor', json.dumps(msg_result))


if __name__ == '__main__':
    api = get_api()
    start_server(api)
