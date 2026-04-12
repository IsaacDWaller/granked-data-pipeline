import json
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
            language
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        ),
    )


def get_comments_to_analyse(link_id):
    return fetchall(
        """
        SELECT
            id,
            created_utc,
            parent_id,
            score,
            body,
            depth,
            language,
            adds_information,
            insight_score
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
            language = ?
        WHERE id = ?
        """,
        (
            total_awards_received,
            score,
            body,
            language,
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


def extract_comment(comment):
    execute(
        """
        UPDATE comment
        SET
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
            miscellaneous = ?
        WHERE id = ?
        """,
        (
            comment["brand"],
            comment["model"],
            comment["category"],
            comment["context"],
            json.dumps(comment["attributes"]),
            comment["price"],
            comment["currency"],
            comment["sentiment"],
            json.dumps(comment["positives"]),
            json.dumps(comment["negatives"]),
            json.dumps(comment["miscellaneous"]),
            comment["id"],
        ),
    )
