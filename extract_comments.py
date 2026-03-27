import json
import logging
import os
import re
import time
from collections import defaultdict
from typing import TextIO

from dotenv import load_dotenv

from utilities import create_connection, generate_chat_completion, load_model

MINIMUM_INSIGHT_SCORE = 7
MAXIMUM_TOKENS = 7_000

load_dotenv()

connection = create_connection(os.getenv("DATABASE_PATH"))
cursor = connection.cursor()

llm = load_model(os.getenv("LLM_MODEL_PATH"))
llm_model = os.path.basename(llm.model_path)

logging.basicConfig(filename="extract_comments.log", level=logging.INFO)
logger = logging.getLogger(__name__)

while True:
    link_to_extract = cursor.execute(
        """
        SELECT l.id, l.selftext, l.title
        FROM link l
        WHERE l.id = (
            SELECT c.link_id
            FROM comment c
            WHERE c.depth = 0
            AND c.triaged_at_utc IS NOT NULL
            AND (c.extracted_at_utc IS NULL OR c.extraction_model != ?)
            LIMIT 1
        )
        LIMIT 1
        """,
        (llm_model,),
    ).fetchone()

    if not link_to_extract:
        break

    (link_id, link_selftext, link_title) = link_to_extract

    comments_to_extract = cursor.execute(
        """
        SELECT
            id,
            created_utc,
            parent_id,
            body,
            depth,
            adds_information,
            insight_score
        FROM comment
        WHERE link_id = ?
        ORDER BY depth DESC, created_utc
        """,
        (link_id,),
    ).fetchall()

    comments = {}
    children = defaultdict(list)

    for comment_to_extract in comments_to_extract:
        comment = dict(comment_to_extract)
        comments[comment["id"]] = comment
        children[comment["parent_id"]].append(comment["id"])

    has_extract = {}

    for comment_to_extract in comments_to_extract:
        (
            id,
            created_utc,
            parent_id,
            body,
            depth,
            adds_information,
            insight_score,
        ) = comment_to_extract

        has_extract[id] = (
            adds_information is not None
            and adds_information >= 1
            and insight_score is not None
            and insight_score >= MINIMUM_INSIGHT_SCORE
        ) or any(has_extract[child_id] for child_id in children[id])

    top_level_comment_ids = []

    for comment in comments.values():
        (id, depth) = (comment[key] for key in ("id", "depth"))

        if depth <= 0:
            top_level_comment_ids.append(id)

    batches = []

    for top_level_comment_id in top_level_comment_ids:
        if not has_extract.get(top_level_comment_id):
            continue

        subtree = []
        stack = [top_level_comment_id]

        while stack:
            comment_id = stack.pop()

            if not has_extract.get(comment_id):
                continue

            comment = comments[comment_id].copy()

            (adds_information, insight_score) = (
                comment[key] for key in ("adds_information", "insight_score")
            )

            comment["extract"] = (
                adds_information is not None
                and adds_information >= 1
                and insight_score is not None
                and insight_score >= MINIMUM_INSIGHT_SCORE
            )

            subtree.append(comment)

            for child_id in children[comment_id]:
                if has_extract.get(child_id):
                    stack.append(child_id)

        subtree.sort(key=lambda x: x["created_utc"])

        current_batch = []
        current_tokens = 0

        for comment in subtree:
            tokens = len(comment["body"]) / 4

            if current_tokens + tokens > MAXIMUM_TOKENS and current_batch:
                batches.append(current_batch)
                current_batch = []
                current_tokens = 0

            current_batch.append(comment)
            current_tokens += tokens

        if current_batch:
            batches.append(current_batch)

    for batch in batches:
        llm_input = {
            "link": {"id": link_id, "title": link_title, "selftext": link_selftext},
            "comments": [],
        }

        for comment in batch:
            llm_input["comments"].append(
                {
                    "id": comment["id"],
                    "parent_id": comment["parent_id"],
                    "depth": comment["depth"],
                    "extract": comment["extract"],
                    "body": comment["body"],
                }
            )

        system_prompt: str
        file: TextIO

        with open(os.getenv("EXTRACT_COMMENTS_PROMPT_PATH"), "r") as file:
            system_prompt = file.read()

        response = generate_chat_completion(
            llm,
            system_prompt,
            f"Now extract the same fields for the following:{llm_input}",
        )

        match: re.Match | None = re.search(r"\[\s*{.*?}\s*\]", response, re.DOTALL)

        if not match:
            logging.error(
                f"Extract comments failed link_id={link_id} time={time.time()}"
            )

            continue

        for comment in json.loads(match.group(0)):
            (
                id,
                brand,
                model,
                category,
                context,
                attributes,
                price,
                currency,
                sentiment,
                positives,
                negatives,
                miscellaneous,
            ) = (
                comment[key]
                for key in (
                    "id",
                    "brand",
                    "model",
                    "category",
                    "context",
                    "attributes",
                    "price",
                    "currency",
                    "sentiment",
                    "positives",
                    "negatives",
                    "miscellaneous",
                )
            )

            cursor.execute(
                """
                UPDATE comment
                SET
                    extraction_model = ?,
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
                    miscellaneous = ?,
                    extracted_at_utc = ?
                WHERE id = ?
                """,
                (
                    llm_model,
                    brand,
                    model,
                    category,
                    context,
                    json.dumps(attributes),
                    price,
                    currency,
                    sentiment,
                    json.dumps(positives),
                    json.dumps(negatives),
                    json.dumps(miscellaneous),
                    time.time(),
                    id,
                ),
            )

            connection.commit()
            logger.info(f"Extract comment succeeded id={id} time={time.time()}")

connection.close()
