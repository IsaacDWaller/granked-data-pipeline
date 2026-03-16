import sqlite3
import time

import requests

from utilities import detect_language, sleep

TOTAL_AWARDS_RECEIVED = []
SCORES = []
BODY_LENGTHS = []
DEPTHS = []


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

    TOTAL_AWARDS_RECEIVED.append(total_awards_received)
    SCORES.append(score)
    BODY_LENGTHS.append(len(body))
    DEPTHS.append(depth)

    existing_comment = cursor.execute(
        """
        SELECT total_awards_received, score, body
        FROM comment
        WHERE id = ?
        """,
        (id,),
    ).fetchone()

    if existing_comment is None:
        cursor.execute(
            """
            INSERT INTO comment (id, total_awards_received, created_utc, parent_id, score, body, link_id, depth, language)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                detect_language(body),
            ),
        )

        connection.commit()
    else:
        (
            existing_total_awards_received,
            existing_score,
            existing_body,
        ) = existing_comment

        if (
            existing_total_awards_received != total_awards_received
            or existing_score != score
            or existing_body != body
        ):
            cursor.execute(
                """
                UPDATE comment
                SET total_awards_received = ?, score = ?, body = ?, inference_model = NULL, inferred_at_utc = NULL
                WHERE id = ?
                """,
                (
                    total_awards_received,
                    score,
                    body,
                    id,
                ),
            )

            connection.commit()

    if replies != "":
        for reply in replies["data"]["children"]:
            extract_comment(reply)


connection = sqlite3.connect("granked.db")
cursor = connection.cursor()

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
        inference_model TEXT,
        inferred_at_utc REAL,
        FOREIGN KEY (link_id) REFERENCES link(id)
    ) STRICT
    """
)

links_to_ingest = cursor.execute(
    """
    SELECT id, subreddit
    FROM link
    WHERE
        upvote_ratio >= 0.8 AND
        score >= 24 AND
        num_comments >= 16 AND
        language = "en" AND
        ingested_at IS NULL
    """,
).fetchall()

for link in links_to_ingest:
    link_id, subreddit = link

    while True:
        response = requests.get(
            f"https://www.reddit.com/r/{subreddit}/comments/{link_id}/.json",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
            },
        )

        status_code = response.status_code

        if status_code != requests.codes.ok:
            print(
                f"Link request failed url={response.url} status_code={status_code} time={time.time()}"
            )
            sleep(1, 2)
            continue

        for comment in response[1]["data"]["children"]:
            extract_comment(comment)

# comments_to_infer = cursor.execute(
#     """
#     SELECT id
#     FROM comment
#     WHERE
#         score >= 4 AND
#         body NOT IN ('[deleted]', '[removed]') AND
#         LENGTH(body) >= 24 AND
#         language = 'en'
#     """
# ).fetchall()

connection.close()
