import os
from os.path import join, dirname, abspath
from glob import glob
from datetime import datetime

import pandas as pd
from kafka import KafkaProducer


def get_message(order: int):
    sup_dir_path = join(dirname(dirname(abspath(__file__))), 'result', 'popular_topic')
    sup_dir_name = sorted(os.listdir(sup_dir_path))[-order]
    dir_path     = join(sup_dir_path, sup_dir_name)
    csv_path     = glob(f"{dir_path}/*.csv")[0]
    df           = pd.read_csv(csv_path)

    time = datetime.strptime(sup_dir_name, "%Y-%m-%d_%H-%M-%S").strftime("%y/%m/%d %H:%M:%S")

    msg  = f"[최근 2시간 내 인기 검색어 순위] \n"
    msg += f"{time} \n"
    msg += '\n'.join(f'{rank}.  {title}' for rank, title in enumerate(df.title, start=1))

    return msg


def send_message():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    cur_msg  = get_message(order=1)
    prev_msg = get_message(order=2)

    if cur_msg != prev_msg:
        producer.send('kakao_alarm', cur_msg.encode('utf-8'))


if __name__ == '__main__':
    send_message()
