import logging
import os
import time

from dotenv import load_dotenv

from granked_data_pipeline.utilities import (
    create_connection,
    generate_chat_completion,
    get_json_match,
    get_logging_filename,
    load_model,
    read_file,
)

MINIMUM_SCORE = 4
MINIMUM_BODY_LENGTH = 24
REQUIRED_LANGUAGE = "en"

load_dotenv()

logging.basicConfig(
    filename=get_logging_filename("triage_comments.log"),
    format="%(created)s:%(levelname)s:%(name)s:%(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    connection = create_connection(os.getenv("DATABASE_PATH"))
    cursor = connection.cursor()

    llm = load_model(os.getenv("LLM_MODEL_PATH"))
    llm_model = os.path.basename(llm.model_path)

    while True:
        comments = cursor.execute(
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
            (MINIMUM_SCORE, MINIMUM_BODY_LENGTH, REQUIRED_LANGUAGE, llm_model),
        ).fetchall()

        if not comments:
            break

        response = generate_chat_completion(
            llm,
            read_file(os.getenv("TRIAGE_COMMENTS_PROMPT_PATH")),
            f"Analyse the following comments:{comments}",
        )

        match = get_json_match(response)

        if not match:
            logging.error("Get JSON match failed")
            continue

        for comment in match:
            id = comment["id"]

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
                    llm_model,
                    comment["adds_information"],
                    comment["insight_score"],
                    comment["summary"],
                    time.time(),
                    id,
                ),
            )

            connection.commit()
            logger.info(f"Triage comment succeeded id={id}")

connection.close()
