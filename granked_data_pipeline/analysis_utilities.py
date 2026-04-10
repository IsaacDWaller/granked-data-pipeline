import json
import re
from typing import TextIO

from llama_cpp import Llama


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
