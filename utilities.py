import random
import time

from langdetect import LangDetectException, detect


def detect_language(text):
    try:
        return detect(text)
    except LangDetectException:
        print(f"Language detect failed text={text} time={time.time()}")
        return None


def sleep(minimum_seconds, maximum_seconds):
    time.sleep(random.uniform(minimum_seconds, maximum_seconds))
