import json
import logging
import os

from dotenv import load_dotenv

from database.comment import extract_comment
from database.link import extract_link, get_link_to_extract
from granked_data_pipeline.llm_utilities import (
    generate_chat_completion,
    get_llm_model,
    get_tokens,
    load_model,
)
from granked_data_pipeline.prompt_creator import create_user_prompts
from granked_data_pipeline.prompt_utilities import (
    can_analyse_link,
    get_system_prompt,
)
from granked_data_pipeline.utilities import (
    get_json_match,
    get_logging_filename,
)

MINIMUM_INSIGHT_SCORE = 7

MAXIMUM_RESPONSE_TOKENS = 2_048

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

    system_prompt = get_system_prompt("extract_comments.txt")
    system_prompt_tokens = get_tokens(llm, system_prompt)

    while True:
        link = get_link_to_extract()

        if not link:
            break

        if not can_analyse_link(
            logger, llm, system_prompt, link, MAXIMUM_RESPONSE_TOKENS, extract_link
        ):
            continue

        user_prompts = create_user_prompts(
            logger,
            llm,
            system_prompt,
            link,
            MAXIMUM_RESPONSE_TOKENS,
            comment_can_be_extracted,
        )

        for index, prompt in enumerate(user_prompts):
            user_prompt_text = json.dumps(prompt)

            response = generate_chat_completion(
                llm,
                system_prompt,
                user_prompt_text,
            )

            match = get_json_match(response)

            if not match:
                logger.error(
                    f"Extraction failed link_id={link["id"]} user_prompt_index={index} system_prompt_tokens={system_prompt_tokens} user_prompt_tokens={get_tokens(llm, user_prompt_text)} response_tokens={get_tokens(llm, response)} total_tokens={get_tokens(llm, f"{system_prompt}{user_prompt_text}{response}")}"
                )

                continue

            for comment in match:
                extract_comment(comment)

            logger.info(
                f"Extraction succeeded link_id={link["id"]} user_prompt_index={index} system_prompt_tokens={system_prompt_tokens} user_prompt_tokens={get_tokens(llm, user_prompt_text)} response_tokens={get_tokens(llm, response)} total_tokens={get_tokens(llm, f"{system_prompt}{user_prompt_text}{response}")}"
            )

        extract_link(get_llm_model(llm), link["id"])
