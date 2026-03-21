import sqlite3
import time

import requests

from utilities import detect_language, sleep

connection = sqlite3.connect("database\\granked.db")
cursor = connection.cursor()

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
        inference_model TEXT,
        inferred_at_utc REAL
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
            print(
                f"Search request failed url={response.url} status_code={status_code} time={time.time()}"
            )

            sleep(2, 4)
            continue

        print(
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

            existing_link = cursor.execute(
                """
                SELECT selftext, title, upvote_ratio, total_awards_received, score, num_comments
                FROM link
                WHERE id = ?
                """,
                (id,),
            ).fetchone()

            if existing_link is None:
                cursor.execute(
                    """
                    INSERT INTO link (id, subreddit, selftext, title, upvote_ratio, total_awards_received, score, num_comments, created_utc, language)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        detect_language(f"{title} {selftext}"),
                    ),
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
                existing_selftext != selftext
                or existing_title != title
                or existing_upvote_ratio != upvote_ratio
                or existing_total_awards_received != total_awards_received
                or existing_score != score
                or existing_num_comments != num_comments
            ):
                cursor.execute(
                    """
                    UPDATE link
                    SET selftext = ?, title = ?, upvote_ratio = ?, total_awards_received = ?, score = ?, num_comments = ?, inference_model = NULL, inferred_at_utc = NULL
                    WHERE id = ?
                    """,
                    (
                        selftext,
                        title,
                        upvote_ratio,
                        total_awards_received,
                        score,
                        num_comments,
                        id,
                    ),
                )

                connection.commit()

        sleep(2, 4)
        break

connection.close()
