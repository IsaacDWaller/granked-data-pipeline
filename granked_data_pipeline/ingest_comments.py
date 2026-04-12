import logging

import requests
from dotenv import load_dotenv

from database.comment import create_comment, get_existing_comment, update_comment
from database.link import (
    clear_triaged_links_by_comment_id,
    get_uningested_links,
)
from granked_data_pipeline.ingestion_utilities import (
    detect_language,
    extract_data,
    sleep,
)
from granked_data_pipeline.utilities import (
    get_logging_filename,
)

MINIMUM_SCORE = 4
MINIMUM_BODY_LENGTH = 24
REQUIRED_LANGUAGE = "en"

load_dotenv()

logging.basicConfig(
    filename=get_logging_filename("ingest_comments.log"),
    format="%(created)s:%(levelname)s:%(name)s:%(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)


def comment_is_valid(score, body, language):
    return (
        score >= MINIMUM_SCORE
        and len(body) >= MINIMUM_BODY_LENGTH
        and language == REQUIRED_LANGUAGE
    )


def extract_comment(comment):
    if comment["kind"] != "t1":
        return

    (
        total_awards_received,
        replies,
        id,
        created_utc,
        parent_id,
        score,
        body,
        link_id,
        depth,
    ) = [
        comment["data"][key]
        for key in [
            "total_awards_received",
            "replies",
            "id",
            "created_utc",
            "parent_id",
            "score",
            "body",
            "link_id",
            "depth",
        ]
    ]

    language = detect_language(logger, body)
    existing_comment = get_existing_comment(id)

    if existing_comment:
        (
            existing_total_awards_received,
            existing_score,
            existing_body,
            existing_language,
        ) = existing_comment

        if (
            existing_total_awards_received != total_awards_received
            or existing_score != score
            or existing_body != body
            or existing_language != language
        ):
            update_comment(total_awards_received, score, body, language, id)

        if (
            existing_body != body
            or (
                comment_is_valid(score, body, language)
                and not comment_is_valid(
                    existing_score, existing_body, existing_language
                )
            )
            or (
                not comment_is_valid(score, body, language)
                and comment_is_valid(existing_score, existing_body, existing_language)
            )
        ):
            clear_triaged_links_by_comment_id(id)

    else:
        create_comment(
            id,
            total_awards_received,
            created_utc,
            parent_id[3:],
            score,
            body,
            link_id[3:],
            depth,
            language,
        )

    if replies != "":
        for reply in replies["data"]["children"]:
            extract_comment(reply)


if __name__ == "__main__":
    links = get_uningested_links()

    for link in links:
        id, subreddit = link

        while True:
            response = extract_data(
                f"https://www.reddit.com/r/{subreddit}/comments/{id}/.json",
                logger,
                "link",
            )

            if response.status_code != requests.codes.ok:
                sleep(2, 4)
                continue

            for comment in response.json()[1]["data"]["children"]:
                extract_comment(comment)

            sleep(1, 2)
            break
