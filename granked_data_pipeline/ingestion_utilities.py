import logging
import random
import time

import requests
from langdetect import LangDetectException, detect


def extract_data(url, logger: logging.Logger, request_type: str, params={}):
    response = requests.get(
        url,
        params=params,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
        },
    )

    (url, status_code) = (response.url, response.status_code)

    if status_code == requests.codes.ok:
        logger.info(
            f"{request_type.capitalize()} request succeeded url={url} status_code={status_code}"
        )
    else:
        logger.error(
            f"{request_type.capitalize()} request failed url={url} status_code={status_code}"
        )

    return response


def detect_language(logger: logging.Logger, text):
    try:
        return detect(text)
    except LangDetectException:
        logger.error(f"Language detect failed text={text}")
        return None


def sleep(minimum_seconds, maximum_seconds):
    time.sleep(random.uniform(minimum_seconds, maximum_seconds))
