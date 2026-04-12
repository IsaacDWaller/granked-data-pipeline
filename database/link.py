import time

from database.database import execute, fetchall, fetchone


def create_link(
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
):
    execute(
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


def get_unextracted_links():
    return fetchall(
        """
        SELECT id, subreddit
        FROM link
        WHERE extracted_at_utc IS NULL
        """,
    )


def get_link(link_id):
    return fetchone(
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
        (link_id,),
    )


def get_untriaged_link():
    return fetchone(
        """
        SELECT id, selftext, title
        FROM link
        WHERE triaged_at_utc IS NULL
        LIMIT 1
        """,
    )


def update_link(
    selftext, title, upvote_ratio, total_awards_received, score, num_comments, id
):
    execute(
        """
        UPDATE link
        SET
            selftext = ?,
            title = ?,
            upvote_ratio = ?,
            total_awards_received = ?,
            score = ?,
            num_comments = ?,
            ingested_at_utc = ?
        WHERE id = ?
        """,
        (
            selftext,
            title,
            upvote_ratio,
            total_awards_received,
            score,
            num_comments,
            time.time(),
            id,
        ),
    )


def triage_link(llm_model, link_id):
    execute(
        """
        UPDATE link
        SET triage_model = ?, triaged_at_utc = ?
        WHERE id = ?
        """,
        (
            llm_model,
            time.time(),
            link_id,
        ),
    )


def clear_link_analysis_timestamps(link_id):
    execute(
        """
        UPDATE link
        SET triaged_at_utc = NULL, extracted_at_utc = NULL
        WHERE id = ?
        """,
        (link_id,),
    )


def clear_link_analysis_timestamps_by_comment_id(comment_id):
    execute(
        """
        UPDATE link
        SET triaged_at_utc = NULL, extracted_at_utc = NULL
        WHERE id = (SELECT link_id FROM comment WHERE id = ?)
        """,
        (comment_id,),
    )


def delete_link(link_id):
    execute(
        "DELETE FROM link WHERE id = ?",
        (link_id,),
    )
