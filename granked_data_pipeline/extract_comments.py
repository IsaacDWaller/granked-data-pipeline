import json
import logging
import os
import time
from collections import defaultdict
from typing import Dict

from dotenv import load_dotenv

from granked_data_pipeline.analysis_utilities import (
    generate_chat_completion,
    get_json_match,
    load_model,
    read_file,
)
from granked_data_pipeline.utilities import (
    create_connection,
    get_logging_filename,
)

MINIMUM_INSIGHT_SCORE = 7
MAXIMUM_TOKENS = 7_000

load_dotenv()

logging.basicConfig(
    filename=get_logging_filename("extract_comments.log"),
    format="%(created)s:%(levelname)s:%(name)s:%(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)


def create_prompt_content(link_id, link_selftext, link_title):
    return {
        "link": {
            "id": link_id,
            "title": link_title,
            "selftext": link_selftext,
        },
        "comments": [],
    }


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
    minimum_insight_score,
):
    comment_ids_are_in_prompt = {}

    for comment in comments_by_depth_descending:
        id = comment["id"]

        comment_ids_are_in_prompt[id] = (
            comment_is_insightful(comment, minimum_insight_score)
        ) or any(
            comment_ids_are_in_prompt.get(child_comment_id)
            for child_comment_id in child_comment_ids[id]
        )

    return comment_ids_are_in_prompt


def comment_is_insightful(comment, minimum_insight_score):
    (adds_information, insight_score) = (
        comment[key] for key in ("adds_information", "insight_score")
    )

    return (adds_information or 0) >= 1 and (
        insight_score or 0
    ) >= minimum_insight_score


def get_comment_thread(
    comments_by_id: Dict[str, dict],
    child_comment_ids: dict,
    comment_ids_are_in_prompt: dict,
    comment_id,
):
    if not comment_ids_are_in_prompt.get(comment_id):
        return []

    comment = comments_by_id[comment_id].copy()
    comment["can_be_extracted"] = comment_is_insightful(comment, MINIMUM_INSIGHT_SCORE)
    comment_thread = [comment]

    for child_comment_id in child_comment_ids.get(comment_id, []):
        comment_thread.extend(
            get_comment_thread(
                comments_by_id,
                child_comment_ids,
                comment_ids_are_in_prompt,
                child_comment_id,
            )
        )

    return comment_thread


def get_top_level_comment_ids(comments_by_id: dict):
    return sorted(
        [comment["id"] for comment in comments_by_id.values() if comment["depth"] <= 0],
        key=lambda comment_id: comments_by_id[comment_id]["created_utc"],
    )


if __name__ == "__main__":
    connection = create_connection(os.getenv("DATABASE_PATH"))
    cursor = connection.cursor()

    llm = load_model(os.getenv("LLM_MODEL_PATH"))
    llm_model = os.path.basename(llm.model_path)

    while True:
        link = cursor.execute(
            """
            SELECT l.id, l.selftext, l.title
            FROM link l
            WHERE l.id = (
                SELECT c.link_id
                FROM comment c
                WHERE
                    c.triaged_at_utc IS NOT NULL AND
                    c.adds_information >= 1 AND
                    c.insight_score >= ? AND
                    (c.extracted_at_utc IS NULL OR c.extraction_model != ?)
                LIMIT 1
            )
            LIMIT 1
            """,
            (
                MINIMUM_INSIGHT_SCORE,
                llm_model,
            ),
        ).fetchone()

        if not link:
            break

        (link_id, link_selftext, link_title) = (
            link[key] for key in ("id", "selftext", "title")
        )

        prompt_content = create_prompt_content(link_id, link_selftext, link_title)
        prompt_content_tokens = len(json.dumps(prompt_content)) // 4

        if prompt_content_tokens > MAXIMUM_TOKENS:
            cursor.execute(
                """
                UPDATE link
                SET extracted_at_utc = ?
                WHERE id = ?
                """,
                (time.time(), link_id),
            )

            connection.commit()

            cursor.execute(
                """
                UPDATE comment
                SET extracted_at_utc = ?
                WHERE link_id = ?
                """,
                (time.time(), link_id),
            )

            connection.commit()

            logger.warning(
                f"Link exceeded maximum tokens id={link_id} tokens={prompt_content_tokens}"
            )

            continue

        comments_by_id = {
            comment["id"]: comment
            for comment in cursor.execute(
                """
                SELECT
                    id,
                    created_utc,
                    parent_id,
                    body,
                    depth,
                    adds_information,
                    insight_score
                FROM comment
                WHERE link_id = ?
                ORDER BY depth DESC, created_utc
                """,
                (link_id,),
            ).fetchall()
        }

        child_comment_ids = get_child_comment_ids(comments_by_id)
        comments_by_depth_descending = get_comments_by_depth_descending(comments_by_id)

        comment_ids_are_in_prompt = get_comment_ids_are_in_prompt(
            comments_by_depth_descending,
            child_comment_ids,
            MINIMUM_INSIGHT_SCORE,
        )

        top_level_comment_ids = get_top_level_comment_ids(comments_by_id)
        total_tokens = prompt_content_tokens
        prompt_contents = []

        for id in top_level_comment_ids:
            for comment in get_comment_thread(
                comments_by_id, child_comment_ids, comment_ids_are_in_prompt, id
            ):
                prompt_comment = {
                    "id": comment["id"],
                    "parent_id": comment["parent_id"],
                    "body": comment["body"],
                    "can_be_extracted": comment["can_be_extracted"],
                }

                tokens = len(json.dumps(prompt_comment)) // 4

                if prompt_content_tokens + tokens > MAXIMUM_TOKENS:
                    cursor.execute(
                        """
                        UPDATE comment
                        SET extracted_at_utc = ?
                        WHERE id = ?
                        """,
                        (time.time(), comment["id"]),
                    )

                    connection.commit()

                    logger.warning(
                        f"Comment exceeded maximum tokens id={comment['id']} tokens={tokens}"
                    )

                    continue

                if total_tokens + tokens > MAXIMUM_TOKENS:
                    prompt_contents.append(prompt_content)

                    prompt_content = create_prompt_content(
                        link_id, link_selftext, link_title
                    )

                    total_tokens = prompt_content_tokens

                prompt_content["comments"].append(prompt_comment)
                total_tokens += tokens

        if prompt_content["comments"]:
            prompt_contents.append(prompt_content)

        for content in prompt_contents:
            response = generate_chat_completion(
                llm,
                read_file(os.getenv("EXTRACT_COMMENTS_PROMPT_PATH")),
                f"Now extract the same fields for the following:{content}",
            )

            match = get_json_match(response)

            if not match:
                logging.error(f"Get JSON match failed link_id={link_id}")
                continue

            for comment in match:
                (
                    id,
                    brand,
                    model,
                    category,
                    context,
                    attributes,
                    price,
                    currency,
                    sentiment,
                    positives,
                    negatives,
                    miscellaneous,
                ) = (
                    comment[key]
                    for key in (
                        "id",
                        "brand",
                        "model",
                        "category",
                        "context",
                        "attributes",
                        "price",
                        "currency",
                        "sentiment",
                        "positives",
                        "negatives",
                        "miscellaneous",
                    )
                )

                cursor.execute(
                    """
                    UPDATE comment
                    SET
                        extraction_model = ?,
                        brand = ?,
                        model = ?,
                        category = ?,
                        context = ?,
                        attributes = ?,
                        price = ?,
                        currency = ?,
                        sentiment = ?,
                        positives = ?,
                        negatives = ?,
                        miscellaneous = ?,
                        extracted_at_utc = ?
                    WHERE id = ?
                    """,
                    (
                        llm_model,
                        brand,
                        model,
                        category,
                        context,
                        json.dumps(attributes),
                        price,
                        currency,
                        sentiment,
                        json.dumps(positives),
                        json.dumps(negatives),
                        json.dumps(miscellaneous),
                        time.time(),
                        id,
                    ),
                )

                connection.commit()
                logger.info(f"Extract comment succeeded id={id}")

    connection.close()
