from realtime_trend_pipeline.common import *


def producer(text, output_path):
    yield (None, json.dumps({"text": text, "output_path": output_path}))
