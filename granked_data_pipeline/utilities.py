import json
import logging
import os
import random
import re
import sqlite3
import time
from typing import TextIO

import requests
from langdetect import LangDetectException, detect
from llama_cpp import Llama


def create_connection(database):
    connection = sqlite3.connect(database)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def get_logging_filename(file_path):
    folder_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs"
    )

    os.makedirs(folder_path, exist_ok=True)
    return os.path.join(folder_path, file_path)


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


def load_model(
    model_path,
    n_gpu_layers=-1,
    n_ctx=8_192,
    n_batch=1_024,
    n_threads=12,
    flash_attn=True,
    op_offload=True,
    verbose=False,
):
    return Llama(
        model_path=model_path,
        n_gpu_layers=n_gpu_layers,
        n_ctx=n_ctx,
        n_batch=n_batch,
        n_ubatch=n_batch,
        n_threads=n_threads,
        n_threads_batch=n_threads,
        flash_attn=flash_attn,
        op_offload=op_offload,
        verbose=verbose,
    )


def read_file(file):
    stream: TextIO

    with open(file, "r") as stream:
        return stream.read()


def generate_chat_completion(llm: Llama, system_prompt, user_prompt):
    return llm.create_chat_completion(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )["choices"][0]["message"]["content"]


def get_json_match(string):
    match: re.Match | None = re.search(r"\[\s*{.*?}\s*\]", string, re.DOTALL)

    if not match:
        return None

    return json.loads(match.group())
