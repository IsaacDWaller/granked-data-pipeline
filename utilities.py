import logging
import random
import sqlite3
import time

from langdetect import LangDetectException, detect
from llama_cpp import Llama


def create_connection(database):
    connection = sqlite3.connect(database)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def detect_language(logger: logging.Logger, text):
    try:
        return detect(text)
    except LangDetectException:
        logger.error(f"Language detect failed text={text} time={time.time()}")
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


def generate_chat_completion(llm: Llama, system_prompt, user_prompt):
    return llm.create_chat_completion(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )["choices"][0]["message"]["content"]
