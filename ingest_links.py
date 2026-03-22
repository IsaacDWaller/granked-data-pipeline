import logging
import time

import requests

from utilities import create_connection, detect_language, sleep

MINIMUM_UPVOTE_RATIO = 0.8
MINIMUM_SCORE = 24
MINIMUM_NUM_COMMENTS = 16
REQUIRED_LANGUAGE = "en"

connection = create_connection("database\\granked.db")
cursor = connection.cursor()

logger = logging.getLogger(__name__)
logging.basicConfig(filename="ingest_links.log", level=logging.INFO)

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS link (
        id TEXT PRIMARY KEY,
        subreddit TEXT NOT NULL,
        selftext TEXT NOT NULL,
        title TEXT NOT NULL,
        upvote_ratio REAL NOT NULL,
        total_awards_received INTEGER NOT NULL,
        score INTEGER NOT NULL,
        num_comments INTEGER NOT NULL,
        created_utc INTEGER NOT NULL,
        language TEXT,
        ingested_at_utc REAL NOT NULL,
        triage_model TEXT,
        triaged_at_utc REAL
    ) STRICT
    """
)

for query in ["best mechanical keyboard"]:
    while True:
        response = requests.get(
            "https://www.reddit.com/search.json",
            params={"q": query, "limit": 100, "include_over_18": True},
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
            },
        )

        status_code = response.status_code

        if status_code != requests.codes.ok:
            logger.error(
                f"Search request failed url={response.url} status_code={status_code} time={time.time()}"
            )

            sleep(2, 4)
            continue

        logger.info(
            f"Search request succeeded url={response.url} status_code={status_code} time={time.time()}"
        )

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

            existing_link = cursor.execute(
                """
                SELECT
                    selftext,
                    title,
                    upvote_ratio,
                    total_awards_received,
                    score,
                    num_comments
                FROM link
                WHERE id = ?
                """,
                (id,),
            ).fetchone()

            if existing_link:
                if (
                    upvote_ratio < MINIMUM_UPVOTE_RATIO
                    or score < MINIMUM_SCORE
                    or num_comments < MINIMUM_NUM_COMMENTS
                    or language != REQUIRED_LANGUAGE
                ):
                    cursor.execute(
                        "DELETE FROM link WHERE id = ?",
                        (id,),
                    )

                    connection.commit()
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
                    existing_upvote_ratio != upvote_ratio
                    or existing_total_awards_received != total_awards_received
                    or existing_score != score
                    or existing_num_comments != num_comments
                ):
                    cursor.execute(
                        """
                        UPDATE link
                        SET
                            upvote_ratio = ?,
                            total_awards_received = ?,
                            score = ?,
                            num_comments = ?,
                            ingested_at_utc = ?
                        WHERE id = ?
                        """,
                        (
                            upvote_ratio,
                            total_awards_received,
                            score,
                            num_comments,
                            time.time(),
                            id,
                        ),
                    )

                    connection.commit()

                if existing_selftext != selftext or existing_title != title:
                    cursor.execute(
                        """
                        UPDATE link
                        SET
                            selftext = ?,
                            title = ?,
                            ingested_at_utc = ?,
                            triaged_at_utc = NULL
                        WHERE id = ?
                        """,
                        (
                            selftext,
                            title,
                            time.time(),
                            id,
                        ),
                    )

                    connection.commit()
            elif (
                not existing_link
                and upvote_ratio >= MINIMUM_UPVOTE_RATIO
                and score >= MINIMUM_SCORE
                and num_comments >= MINIMUM_NUM_COMMENTS
                and language == REQUIRED_LANGUAGE
            ):
                cursor.execute(
                    """
                    INSERT INTO link (
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
                        ingested_at_utc
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
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
                        time.time(),
                    ),
                )

                connection.commit()

        sleep(2, 4)
        break

connection.close()
