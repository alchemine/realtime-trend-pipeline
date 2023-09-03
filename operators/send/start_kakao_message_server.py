from os.path import join, dirname, basename, abspath
from time import sleep
import json
from glob import glob
from datetime import datetime

import pandas as pd
from PyKakao import Message
from kafka import KafkaConsumer


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


def start_server(api):
    consumer = KafkaConsumer(
        'recent_topics',
        bootstrap_servers=['kafka-server:19092'],
        group_id='kakao_message_server',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=False
    )

    for msg in consumer:
        print("- Message info")
        print(f"topic={msg.topic}, partition={msg.partition}, offset={msg.offset}")
        print(f"key={msg.key}, value={msg.value}")

        text, output_path = msg.value['text'], msg.value['output_path']
        api.send_text(text=text, link={}, button_title="바로 확인")
        consumer.commit()


if __name__ == '__main__':
    api = get_api()
    start_server(api)
