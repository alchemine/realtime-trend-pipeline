from realtime_trend_pipeline.common import *


def producer(text, output_path):
    yield (
        json.dumps(0),
        json.dumps(
            {
                "text": text,
                "output_path": output_path
            }
        )
    )
