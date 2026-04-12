import logging

import requests
from dotenv import load_dotenv

from database.link import (
    clear_link_analysis_timestamps,
    create_link,
    delete_link,
    get_link,
    update_link,
)
from granked_data_pipeline.ingestion_utilities import (
    detect_language,
    extract_data,
    sleep,
)
from granked_data_pipeline.utilities import (
    get_logging_filename,
)

load_dotenv()

MINIMUM_UPVOTE_RATIO = 0.8
MINIMUM_SCORE = 24
MINIMUM_NUM_COMMENTS = 16
REQUIRED_LANGUAGE = "en"


if __name__ == "__main__":
    logging.basicConfig(
        filename=get_logging_filename("ingest_links.log"),
        format="%(created)s:%(levelname)s:%(name)s:%(message)s",
        level=logging.INFO,
    )

    logger = logging.getLogger(__name__)

    for query in ["best mechanical keyboard"]:
        while True:
            response = extract_data(
                "https://www.reddit.com/search.json",
                logger,
                "search",
                params={"q": query, "limit": 100, "include_over_18": True},
            )

            if response.status_code != requests.codes.ok:
                sleep(2, 4)
                continue

            for link in response.json()["data"]["children"]:
                if link["kind"] != "t3":
                    continue

                (
                    subreddit,
                    selftext,
                    title,
                    upvote_ratio,
                    total_awards_received,
                    score,
                    id,
                    num_comments,
                    created_utc,
                ) = [
                    link["data"][key]
                    for key in [
                        "subreddit",
                        "selftext",
                        "title",
                        "upvote_ratio",
                        "total_awards_received",
                        "score",
                        "id",
                        "num_comments",
                        "created_utc",
                    ]
                ]

                language = detect_language(logger, f"{title} {selftext}")
                existing_link = get_link(id)

                if existing_link:
                    if (
                        upvote_ratio < MINIMUM_UPVOTE_RATIO
                        or score < MINIMUM_SCORE
                        or num_comments < MINIMUM_NUM_COMMENTS
                        or language != REQUIRED_LANGUAGE
                    ):
                        delete_link(id)
                        continue

                    (
                        existing_selftext,
                        existing_title,
                        existing_upvote_ratio,
                        existing_total_awards_received,
                        existing_score,
                        existing_num_comments,
                    ) = existing_link

                    if (
                        existing_selftext != selftext
                        or existing_title != title
                        or existing_upvote_ratio != upvote_ratio
                        or existing_total_awards_received != total_awards_received
                        or existing_score != score
                        or existing_num_comments != num_comments
                    ):
                        update_link(
                            selftext,
                            title,
                            upvote_ratio,
                            total_awards_received,
                            score,
                            num_comments,
                            id,
                        )

                    if existing_selftext != selftext or existing_title != title:
                        clear_link_analysis_timestamps(id)

                elif (
                    not existing_link
                    and upvote_ratio >= MINIMUM_UPVOTE_RATIO
                    and score >= MINIMUM_SCORE
                    and num_comments >= MINIMUM_NUM_COMMENTS
                    and language == REQUIRED_LANGUAGE
                ):
                    create_link(
                        id,
                        subreddit,
                        selftext,
                        title,
                        upvote_ratio,
                        total_awards_received,
                        score,
                        num_comments,
                        created_utc,
                        language,
                    )

            sleep(2, 4)
            break
