from realtime_trend_pipeline.common import *

from datetime import datetime


def process_output_path_to_message(output_path: str):
    input_format  = "%Y-%m-%d_%H-%M-%S"
    output_format = "%y/%m/%d %H:%M:%S"
    
    time     = datetime.strptime(basename(output_path), input_format).strftime(output_format)
    csv_path = glob(f"{output_path}/*.csv")[0]  # *.csv is unique
    df       = pd.read_csv(csv_path)

    text  =  "[최근 1시간 인기 검색어] \n"
    text += f"기준시간: {time} \n"
    text += '\n'.join(f"{rank:>2}.  {title}" for rank, title in enumerate(df.iloc[:, 0], start=1))
    return text
