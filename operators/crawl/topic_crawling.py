from realtime_trend_pipeline.common import *

from datetime import datetime

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By


def get_chromedriver(url):
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-setuid-sandbox')
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(
        # service=Service(executable_path='/opt/google/chromedriver'),
        options=chrome_options
    )
    driver.get(url)
    return driver


def crawl_topic(file_path: str):
    # 1. Get connection
    driver = get_chromedriver("http://signal.bz")
    find_class_name = lambda value: driver.find_elements(By.CLASS_NAME, value)


    # 2. Get date
    raw_date = driver.find_element(by=By.CSS_SELECTOR, value='#app > div > main > div > section > div > section > section:nth-child(2) > div:nth-child(1) > div:nth-child(1) > span').text
    match    = re.search(r"^([\d]+)년 ([\d]+)월 ([\d]+)일 [\D]요일 ([\D]{2}) ([\d]+):([\d]+)$", raw_date)
    year, month, day, ampm, hour, minute = match.groups()
    year, month, day, hour, minute = map(int, [year, month, day, hour, minute])
    hour = hour if (ampm == '오전') or (hour == 12) else hour + 12
    time = datetime(year, month, day, hour, minute)


    # 3. Clean topic
    topics_title = [x.text for x in find_class_name("rank-text")]
    topics_rank  = [int(x.text) for x in find_class_name("rank-num")]
    topics       = [{'time': time, 'rank': rank, 'title': title} for rank, title in zip(topics_rank, topics_title)]
    topics_df    = pd.DataFrame(topics).sort_values(['time', 'rank'])


    # 4. Save to file system(volume)
    os.makedirs(dirname(file_path), exist_ok=True)
    topics_df.to_csv(file_path, index=False)
    print("Load Success:", file_path)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: <script> <file_path>")
        print("- argv:", sys.argv)
        sys.exit(-1)

    crawl_topic(sys.argv[1])
