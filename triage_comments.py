import json
import os
import sqlite3
import time

from llama_cpp import Llama

connection = sqlite3.connect("database\\granked.db")
cursor = connection.cursor()

llm = Llama(
    model_path="C:\\Users\\isaac\\llm\\models\\Meta-Llama-3.1-8B-Instruct-Q5_K_M.gguf",
    n_gpu_layers=-1,
    n_ctx=8_192,
    n_batch=1_024,
    n_ubatch=1_024,
    n_threads=12,
    n_threads_batch=12,
    flash_attn=True,
    op_offload=True,
    verbose=False,
)

model = os.path.basename(llm.model_path)

while True:
    comments_to_triage = cursor.execute(
        """
        SELECT id, body
        FROM comment
        WHERE
            score >= 4 AND
            LENGTH(body) >= 24 AND
            language = "en" AND
            (inference_triage_model IS NULL OR inference_triage_model != ?) AND
            inference_triaged_at_utc IS NULL
        LIMIT 8
        """,
        (model,),
    ).fetchall()

    if not comments_to_triage:
        break

    comments_text = ""
    for comment in comments_to_triage:
        (id, body) = comment
        comments_text += f'\n\n<COMMENT id="{id}">\n{body}\n</COMMENT>'

    system_prompt = """
You are analysing Reddit comments.

Return ONLY valid JSON.

Output format:
[
    {
    "id": string,
    "adds_information": 0 or 1,
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
                inference_triage_model = ?,
                adds_information = ?,
                insight_score = ?,
                summary = ?,
                inference_triaged_at_utc = ?
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
