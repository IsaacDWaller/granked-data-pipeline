import json
import logging
from collections import defaultdict
from typing import Callable, Dict

from llama_cpp import Llama

from database.comment import get_comments_to_analyse
from granked_data_pipeline.llm_utilities import get_tokens, prompt_exceeds_tokens
from granked_data_pipeline.prompt_utilities import (
    create_user_prompt,
    create_user_prompt_comment,
)


def create_user_prompts(
    logger: logging.Logger,
    llm: Llama,
    system_prompt: str,
    link: dict,
    maximum_response_tokens: int,
    comment_can_be_analysed: Callable[[dict], bool],
) -> list[dict]:
    comments_by_id = {
        comment["id"]: comment for comment in get_comments_to_analyse(link["id"])
    }

    child_comment_ids = get_child_comment_ids(comments_by_id)

    comments_by_depth_descending = sorted(
        comments_by_id.values(), key=lambda comment: comment["depth"], reverse=True
    )

    user_prompt = create_user_prompt(link["id"], link["selftext"], link["title"])
    user_prompts = []

    for id in [
        comment["id"] for comment in comments_by_id.values() if comment["depth"] <= 0
    ]:
        for comment in get_comment_thread(
            comments_by_id,
            child_comment_ids,
            get_comment_ids_are_in_prompt(
                comments_by_depth_descending,
                child_comment_ids,
                comment_can_be_analysed,
            ),
            id,
            comment_can_be_analysed,
        ):
            prompt_comment = create_user_prompt_comment(comment)
            user_prompt["comments"].append(prompt_comment)
            user_prompt_text = json.dumps(user_prompt)

            if len(user_prompt["comments"]) <= 1 and prompt_exceeds_tokens(
                llm,
                f"{system_prompt}{user_prompt_text}",
                maximum_response_tokens,
            ):
                user_prompt["comments"].pop()

                logger.warning(
                    f"Comment exceeded maximum tokens id={comment["id"]} system_prompt_tokens={get_tokens(llm, system_prompt)} user_prompt_tokens={get_tokens(llm, user_prompt_text)}"
                )

                continue

            if prompt_exceeds_tokens(
                llm, f"{system_prompt}{user_prompt_text}", maximum_response_tokens
            ):
                user_prompt["comments"].pop()
                user_prompts.append(user_prompt)

                user_prompt = create_user_prompt(
                    link["id"], link["selftext"], link["title"]
                )

            user_prompt["comments"].append(prompt_comment)

    if user_prompt["comments"]:
        user_prompts.append(user_prompt)

    return user_prompts


def get_child_comment_ids(comments_by_id: Dict[dict]) -> Dict[str, list[str]]:
    child_comment_ids = defaultdict(list)

    for comment in comments_by_id.values():
        child_comment_ids[comment["parent_id"]].append(comment["id"])

    for comments in child_comment_ids.values():
        comments.sort(key=lambda content_id: comments_by_id[content_id]["created_utc"])

    return child_comment_ids


def get_comment_ids_are_in_prompt(
    comments_by_depth_descending: list[dict],
    child_comment_ids: Dict[str, list[str]],
    comment_can_be_analysed: Callable[[dict], bool],
) -> Dict[str, bool]:
    comment_ids_are_in_prompt = {}

    for comment in comments_by_depth_descending:
        id = comment["id"]

        comment_ids_are_in_prompt[id] = comment_can_be_analysed(comment) or any(
            comment_ids_are_in_prompt.get(child_comment_id)
            for child_comment_id in child_comment_ids[id]
        )

    return comment_ids_are_in_prompt


def get_comment_thread(
    comments_by_id: Dict[str, dict],
    child_comment_ids: Dict[str, list[str]],
    comment_ids_are_in_prompt: Dict[str, bool],
    comment_id: str,
    comment_can_be_analysed: Callable[[dict], bool],
) -> list[dict]:
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
