import json
import os
import re
from collections import defaultdict
from typing import Callable, Dict

from llama_cpp import Llama

CONTEXT_WINDOW = 8_192
MAXIMUM_RESPONSE_TOKENS = 2_048


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


def create_user_prompt(link_id, link_selftext, link_title):
    return {
        "link": {
            "id": link_id,
            "title": link_title,
            "selftext": link_selftext,
        },
        "comments": [],
    }


def get_system_prompt(prompt_path):
    prompts_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "prompts"
    )

    with open(os.path.join(prompts_path, prompt_path), "r") as stream:
        return stream.read()


def get_child_comment_ids(comments_by_id: dict):
    child_comment_ids = defaultdict(list)

    for comment in comments_by_id.values():
        child_comment_ids[comment["parent_id"]].append(comment["id"])

    for comments in child_comment_ids.values():
        comments.sort(key=lambda content_id: comments_by_id[content_id]["created_utc"])

    return child_comment_ids


def get_comments_by_depth_descending(comments_by_id: dict):
    return sorted(
        comments_by_id.values(), key=lambda comment: comment["depth"], reverse=True
    )


def get_comment_ids_are_in_prompt(
    comments_by_depth_descending: dict,
    child_comment_ids: dict,
    comment_is_in_prompt: Callable[[dict], bool],
):
    comment_ids_are_in_prompt = {}

    for comment in comments_by_depth_descending:
        id = comment["id"]

        comment_ids_are_in_prompt[id] = comment_is_in_prompt(comment) or any(
            comment_ids_are_in_prompt.get(child_comment_id)
            for child_comment_id in child_comment_ids[id]
        )

    return comment_ids_are_in_prompt


def get_top_level_comment_ids(comments_by_id: dict):
    return sorted(
        [comment["id"] for comment in comments_by_id.values() if comment["depth"] <= 0],
        key=lambda comment_id: comments_by_id[comment_id]["created_utc"],
    )


def get_comment_thread(
    comments_by_id: Dict[str, dict],
    child_comment_ids: dict,
    comment_ids_are_in_prompt: dict,
    comment_id,
    comment_can_be_analysed: Callable[[dict], bool],
):
    if not comment_ids_are_in_prompt.get(comment_id):
        return []

    comment = dict(comments_by_id[comment_id])
    comment["can_be_analysed"] = comment_can_be_analysed(comment)
    comment_thread = [comment]

    for child_comment_id in child_comment_ids.get(comment_id, []):
        comment_thread.extend(
            get_comment_thread(
                comments_by_id,
                child_comment_ids,
                comment_ids_are_in_prompt,
                child_comment_id,
                comment_can_be_analysed,
            )
        )

    return comment_thread


def create_prompt_comment(comment):
    return {
        "id": comment["id"],
        "parent_id": comment["parent_id"],
        "body": comment["body"],
        "can_be_analysed": comment["can_be_analysed"],
    }


def prompt_exceeds_tokens(llm: Llama, prompt: str):
    return get_tokens(llm, prompt) + MAXIMUM_RESPONSE_TOKENS > CONTEXT_WINDOW


def get_tokens(llm: Llama, text: str):
    return len(llm.tokenize(text.encode("utf-8")))


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
