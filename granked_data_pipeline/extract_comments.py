import json
import logging
import os

from dotenv import load_dotenv

from database.comment import get_comments_to_analyse
from database.link import extract_link, get_link_to_extract
from granked_data_pipeline.analysis_utilities import (
    create_prompt_comment,
    create_user_prompt,
    generate_chat_completion,
    get_child_comment_ids,
    get_comment_ids_are_in_prompt,
    get_comment_thread,
    get_comments_by_depth_descending,
    get_json_match,
    get_system_prompt,
    get_tokens,
    get_top_level_comment_ids,
    load_model,
    prompt_exceeds_tokens,
)
from granked_data_pipeline.ingest_comments import extract_comment
from granked_data_pipeline.utilities import (
    get_logging_filename,
)

MINIMUM_INSIGHT_SCORE = 7

load_dotenv()

logging.basicConfig(
    filename=get_logging_filename("extract_comments.log"),
    format="%(created)s:%(levelname)s:%(name)s:%(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)


def comment_can_be_extracted(comment):
    return ((comment["adds_information"] or 0) >= 1) and (
        (comment["insight_score"] or 0) >= MINIMUM_INSIGHT_SCORE
    )


if __name__ == "__main__":
    llm = load_model(os.getenv("LLM_MODEL_PATH"))
    llm_model = os.path.basename(llm.model_path)

    system_prompt = get_system_prompt("extract_comments.txt")
    system_prompt_tokens = get_tokens(llm, system_prompt)

    while True:
        link = get_link_to_extract()

        if not link:
            break

        user_prompt = create_user_prompt(link["id"], link["selftext"], link["title"])
        user_prompt_string = json.dumps(user_prompt)

        if prompt_exceeds_tokens(llm, f"{system_prompt}{user_prompt_string}"):
            extract_link(llm_model, link["id"])

            logger.warning(
                f"Link exceeded maximum tokens id={link["id"]} system_prompt_tokens={system_prompt_tokens} user_prompt_tokens={get_tokens(llm, user_prompt_string)}"
            )

            continue

        comments_by_id = {
            comment["id"]: comment for comment in get_comments_to_analyse(link["id"])
        }

        child_comment_ids = get_child_comment_ids(comments_by_id)
        comments_by_depth_descending = get_comments_by_depth_descending(comments_by_id)

        user_prompt = create_user_prompt(link["id"], link["selftext"], link["title"])
        user_prompts = []

        for id in get_top_level_comment_ids(comments_by_id):
            for comment in get_comment_thread(
                comments_by_id,
                child_comment_ids,
                get_comment_ids_are_in_prompt(
                    comments_by_depth_descending,
                    child_comment_ids,
                    comment_can_be_extracted,
                ),
                id,
                comment_can_be_extracted,
            ):
                prompt_comment = create_prompt_comment(comment)
                user_prompt["comments"].append(prompt_comment)
                user_prompt_string = json.dumps(user_prompt)

                if len(user_prompt["comments"]) <= 1 and prompt_exceeds_tokens(
                    llm,
                    f"{system_prompt}{user_prompt_string}",
                ):
                    user_prompt["comments"].pop()

                    logger.warning(
                        f"Comment exceeded maximum tokens id={comment["id"]} system_prompt_tokens={system_prompt_tokens} user_prompt_tokens={get_tokens(llm, user_prompt_string)}"
                    )

                    continue

                if prompt_exceeds_tokens(
                    llm,
                    f"{system_prompt}{user_prompt_string}",
                ):
                    user_prompt["comments"].pop()
                    user_prompts.append(user_prompt)

                    user_prompt = create_user_prompt(
                        link["id"], link["selftext"], link["title"]
                    )

                user_prompt["comments"].append(prompt_comment)

        if user_prompt["comments"]:
            user_prompts.append(user_prompt)

        for index, prompt in enumerate(user_prompts):
            user_prompt_string = json.dumps(prompt)

            response = generate_chat_completion(
                llm,
                system_prompt,
                user_prompt_string,
            )

            match = get_json_match(response)

            if not match:
                logger.error(
                    f"Extraction failed link_id={link["id"]} user_prompt_index={index} system_prompt_tokens={system_prompt_tokens} user_prompt_tokens={get_tokens(llm, user_prompt_string)} response_tokens={get_tokens(llm, response)} total_tokens={get_tokens(llm, f"{system_prompt}{user_prompt_string}{response}")}"
                )

                continue

            for comment in match:
                extract_comment(llm_model, comment)

            logger.info(
                f"Extraction succeeded link_id={link["id"]} user_prompt_index={index} system_prompt_tokens={system_prompt_tokens} user_prompt_tokens={get_tokens(llm, user_prompt_string)} response_tokens={get_tokens(llm, response)} total_tokens={get_tokens(llm, f"{system_prompt}{user_prompt_string}{response}")}"
            )

        extract_link(llm_model, link["id"])
