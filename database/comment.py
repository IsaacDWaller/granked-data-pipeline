import time

from database.database import execute, fetchall, fetchone


def create_comment(
    id,
    total_awards_received,
    created_utc,
    parent_id,
    score,
    body,
    link_id,
    depth,
    language,
):
    execute(
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
            parent_id,
            score,
            body,
            link_id,
            depth,
            language,
            time.time(),
        ),
    )


def get_comments_to_triage(link_id):
    return fetchall(
        """
        SELECT
            id,
            created_utc,
            parent_id,
            score,
            body,
            depth,
            language
        FROM comment
        WHERE link_id = ?
        ORDER BY depth DESC, created_utc
        """,
        (link_id,),
    )


def get_existing_comment(comment_id):
    return fetchone(
        """
        SELECT total_awards_received, score, body, language
        FROM comment
        WHERE id = ?
        """,
        (comment_id,),
    )


def update_comment(total_awards_received, score, body, language, id):
    execute(
        """
        UPDATE comment
        SET
            total_awards_received = ?,
            score = ?,
            body = ?,
            language = ?,
            ingested_at_utc = ?
        WHERE id = ?
        """,
        (
            total_awards_received,
            score,
            body,
            language,
            time.time(),
            id,
        ),
    )


def triage_comment(comment):
    execute(
        """
        UPDATE comment
        SET
            adds_information = ?,
            insight_score = ?,
            summary = ?
        WHERE id = ?
        """,
        (
            comment["adds_information"],
            comment["insight_score"],
            comment["summary"],
            comment["id"],
        ),
    )
