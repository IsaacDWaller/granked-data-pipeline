import os
import sqlite3
from contextlib import contextmanager


@contextmanager
def create_connection(commit=False):
    connection = sqlite3.connect(
        os.path.join(
            os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "database"
            ),
            "database.db",
        )
    )

    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")

    try:
        yield connection

        if commit:
            connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def execute(sql, params=()):
    with create_connection(commit=True) as connection:
        connection.execute(sql, params)


def fetchone(sql, params=()):
    with create_connection() as connection:
        return connection.execute(sql, params).fetchone()


def fetchall(sql, params=()):
    with create_connection() as connection:
        return connection.execute(sql, params).fetchall()


if __name__ == "__main__":
    execute(
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
            triaged_at_utc REAL,
            extraction_model TEXT,
            extracted_at_utc REAL
        ) STRICT
        """
    )

    execute(
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
            adds_information INTEGER,
            insight_score INTEGER,
            summary TEXT,
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
            FOREIGN KEY (link_id) REFERENCES link(id) ON DELETE CASCADE
        ) STRICT
        """
    )
