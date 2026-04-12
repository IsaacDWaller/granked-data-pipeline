import json
import logging
import os
from typing import Callable

from llama_cpp import Llama

from granked_data_pipeline.llm_utilities import (
    get_llm_model,
    get_tokens,
    prompt_exceeds_tokens,
)


def get_system_prompt(prompt_path: str) -> str:
    prompts_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "prompts"
    )

    with open(os.path.join(prompts_path, prompt_path), "r") as stream:
        return stream.read()


def create_user_prompt(link_id: str, link_selftext: str, link_title: str) -> dict:
    return {
        "link": {
            "id": link_id,
            "title": link_title,
            "selftext": link_selftext,
        },
        "comments": [],
    }


def create_user_prompt_comment(comment: dict) -> dict:
    return {
        "id": comment["id"],
        "parent_id": comment["parent_id"],
        "body": comment["body"],
        "can_be_analysed": comment["can_be_analysed"],
    }


def can_analyse_link(
    logger: logging.Logger,
    llm: Llama,
    system_prompt: str,
    link: dict,
    maximum_response_tokens: int,
    analyse_link: Callable[[str, int], None],
) -> bool:
    user_prompt = create_user_prompt(link["id"], link["selftext"], link["title"])
    user_prompt_text = json.dumps(user_prompt)

    if prompt_exceeds_tokens(
        llm, f"{system_prompt}{user_prompt_text}", maximum_response_tokens
    ):
        analyse_link(get_llm_model(llm), link["id"])

        logger.warning(
            f"Link exceeded maximum tokens id={link["id"]} system_prompt_tokens={get_tokens(llm, system_prompt)} user_prompt_tokens={get_tokens(llm, user_prompt_text)}"
        )

        return False

    return True
