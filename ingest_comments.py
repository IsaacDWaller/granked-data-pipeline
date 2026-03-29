import logging
import os
import sqlite3
import time

import requests
from dotenv import load_dotenv

from utilities import create_connection, detect_language, sleep

load_dotenv()


def extract_comment(connection: sqlite3.Connection, cursor: sqlite3.Cursor, comment):
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

    existing_comment = cursor.execute(
        """
        SELECT total_awards_received, score, body
        FROM comment
        WHERE id = ?
        """,
        (id,),
    ).fetchone()

    if existing_comment:
        (existing_total_awards_received, existing_score, existing_body) = (
            existing_comment
        )

        if (
            existing_total_awards_received != total_awards_received
            or existing_score != score
        ):
            cursor.execute(
                """
                UPDATE comment
                SET total_awards_received = ?, score = ?, ingested_at_utc = ?
                WHERE id = ?
                """,
                (
                    total_awards_received,
                    score,
                    time.time(),
                    id,
                ),
            )

            connection.commit()

        if existing_body != body:
            cursor.execute(
                """
                UPDATE comment
                SET body = ?, ingested_at_utc = ?, triaged_at_utc = NULL
                WHERE id = ?
                """,
                (
                    body,
                    time.time(),
                    id,
                ),
            )

            connection.commit()
    else:
        cursor.execute(
            """
            INSERT INTO comment (
                id,
                total_awards_received,
                created_utc,
                parent_id,
                score,
                body,
                link_id,
                depth,
                language,
                ingested_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                id,
                total_awards_received,
                created_utc,
                parent_id[3:],
                score,
                body,
                link_id[3:],
                depth,
                language,
                time.time(),
            ),
        )

        connection.commit()

    if replies != "":
        for reply in replies["data"]["children"]:
            extract_comment(connection, cursor, reply)


connection = create_connection(os.getenv("DATABASE_PATH"))
cursor = connection.cursor()

logging.basicConfig(
    filename="ingest_comments.log",
    format="%(created)s:%(levelname)s:%(name)s:%(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS comment (
        id TEXT PRIMARY KEY,
        total_awards_received INTEGER NOT NULL,
        created_utc INTEGER NOT NULL,
        parent_id TEXT NOT NULL,
        score INTEGER NOT NULL,
        body TEXT NOT NULL,
        link_id TEXT NOT NULL,
        depth INTEGER NOT NULL,
        language TEXT,
        ingested_at_utc REAL NOT NULL,
        triage_model TEXT,
        adds_information INTEGER,
        insight_score INTEGER,
        summary TEXT,
        triaged_at_utc REAL,
        extraction_model TEXT,
        brand TEXT,
        model TEXT,
        category TEXT,
        context TEXT,
        attributes TEXT,
        price REAL,
        currency TEXT,
        sentiment TEXT,
        positives TEXT,
        negatives TEXT,
        miscellaneous TEXT,
        extracted_at_utc REAL,
        FOREIGN KEY (link_id) REFERENCES link(id)
    ) STRICT
    """
)

links_to_ingest = cursor.execute(
    """
    SELECT id, subreddit
    FROM link
    WHERE triaged_at_utc IS NULL
    """,
).fetchall()

for link in links_to_ingest:
    id, subreddit = link

    while True:
        response = requests.get(
            f"https://www.reddit.com/r/{subreddit}/comments/{id}/.json",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
            },
        )

        status_code = response.status_code

        if status_code != requests.codes.ok:
            logger.error(
                f"Link request failed url={response.url} status_code={status_code}"
            )

            sleep(1, 2)
            continue

        logger.info(
            f"Link request succeeded url={response.url} status_code={status_code}"
        )

        for comment in response.json()[1]["data"]["children"]:
            extract_comment(connection, cursor, comment)

        sleep(1, 2)
        break

connection.close()
