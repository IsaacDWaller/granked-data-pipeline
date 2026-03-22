import random
import time

from langdetect import LangDetectException, detect
from llama_cpp import Llama


def detect_language(text):
    try:
        return detect(text)
    except LangDetectException:
        print(f"Language detect failed text={text} time={time.time()}")
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
