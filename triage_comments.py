import json
import logging
import os
import time

from dotenv import load_dotenv

from utilities import create_connection, load_model

load_dotenv()

connection = create_connection(os.getenv("DATABASE_PATH"))
cursor = connection.cursor()

llm = load_model(os.getenv("LLM_MODEL_PATH"))

model = os.path.basename(llm.model_path)

logging.basicConfig(filename="triage_comments.log", level=logging.INFO)
logger = logging.getLogger(__name__)

while True:
    comments_to_triage = cursor.execute(
        """
        SELECT id, body
        FROM comment
        WHERE triaged_at_utc IS NULL OR triage_model != ?
        LIMIT 16
        """,
        (model,),
    ).fetchall()

    if not comments_to_triage:
        break

    system_prompt = """
You are an assistant that analyses Reddit comments.

Return ONLY valid JSON.

Output format:
[
    {
        "id": string,
        "adds_information": integer (0/1),
        "insight_score": integer (0-10),
        "summary": string (one sentence)
    }
]

Example:
[
    {
        "id": "abc123",
        "adds_information": 1,
        "insight_score": 7,
        "summary": "Explains why TKL keyboards are preferred for space efficiency."
    }
]

Rules:
- Use the exact id from each comment
- Do not omit any comments
- Do not include any text outside JSON
"""

    comments_text = ""

    for comment in comments_to_triage:
        (id, body) = comment
        comments_text += f"""\n\n<COMMENT id="{id}">\n{body}\n</COMMENT>"""

    response = llm.create_chat_completion(
        messages=[
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": f"Analyse the following comments:{comments_text}",
            },
        ],
    )["choices"][0]["message"]["content"]

    triaged_comments = json.loads(response)

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
        logger.info(f"Triaged comment id={id}")

connection.close()
