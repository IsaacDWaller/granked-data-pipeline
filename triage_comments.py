import json
import logging
import os
import re
import time
from typing import TextIO

from dotenv import load_dotenv

from utilities import create_connection, generate_chat_completion, load_model

MINIMUM_SCORE = 4
MINIMUM_BODY_LENGTH = 24
REQUIRED_LANGUAGE = "en"

load_dotenv()

connection = create_connection(os.getenv("DATABASE_PATH"))
cursor = connection.cursor()

llm = load_model(os.getenv("LLM_MODEL_PATH"))

model = os.path.basename(llm.model_path)

logging.basicConfig(
    filename="triage_comments.log",
    format="%(created)s:%(levelname)s:%(name)s:%(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)

while True:
    comments_to_triage = cursor.execute(
        """
        SELECT id, body
        FROM comment
        WHERE 
            score >= ? AND
            LENGTH(body) >= ? AND
            language = ? AND
            (triaged_at_utc IS NULL OR triage_model != ?)
        LIMIT 16
        """,
        (MINIMUM_SCORE, MINIMUM_BODY_LENGTH, REQUIRED_LANGUAGE, model),
    ).fetchall()

    if not comments_to_triage:
        break

    system_prompt: str
    file: TextIO

    with open(os.getenv("TRIAGE_COMMENTS_PROMPT_PATH"), "r") as file:
        system_prompt = file.read()

    comments_text = ""

    for comment in comments_to_triage:
        (id, body) = comment
        comments_text += f"""\n\n<COMMENT id="{id}">\n{body}\n</COMMENT>"""

    response = generate_chat_completion(
        llm, system_prompt, f"Analyse the following comments:{comments_text}"
    )

    match: re.Match | None = re.search(r"\[\s*{.*?}\s*\]", response, re.DOTALL)

    if not match:
        logging.error("Triage comments failed")
        continue

    triaged_comments = json.loads(match.group(0))

    for comment in triaged_comments:
        (id, adds_information, insight_score, summary) = (
            comment[key]
            for key in ("id", "adds_information", "insight_score", "summary")
        )

        cursor.execute(
            """
            UPDATE comment
            SET
                triage_model = ?,
                adds_information = ?,
                insight_score = ?,
                summary = ?,
                triaged_at_utc = ?
            WHERE id = ?
            """,
            (
                model,
                adds_information,
                insight_score,
                summary,
                time.time(),
                id,
            ),
        )

        connection.commit()
        logger.info(f"Triage comment succeeded id={id}")

connection.close()
