import os

from llama_cpp import Llama

from granked_data_pipeline.constants import CONTEXT_WINDOW


def load_model(
    model_path,
    n_gpu_layers=-1,
    n_ctx=CONTEXT_WINDOW,
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


def get_llm_model(llm: Llama):
    return os.path.basename(llm.model_path)


def prompt_exceeds_tokens(llm: Llama, prompt: str, maximum_response_tokens: int):
    return get_tokens(llm, prompt) + maximum_response_tokens > CONTEXT_WINDOW


def get_tokens(llm: Llama, text: str):
    return len(llm.tokenize(text.encode("utf-8")))


def generate_chat_completion(llm: Llama, system_prompt, user_prompt):
    return llm.create_chat_completion(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )["choices"][0]["message"]["content"]
