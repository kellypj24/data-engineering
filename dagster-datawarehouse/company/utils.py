import functools
import json
import os


@functools.lru_cache
def google_credentials() -> dict:
    return json.loads(os.getenv("GOOGLE_CREDENTIALS", "{}"))


BIGQUERY_URL = "bigquery://fg-eco7rt86wseilpcy95vcc7p8jxl/srx_reporting"
