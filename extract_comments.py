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

llm_model = os.path.basename(llm.model_path)

logging.basicConfig(filename="extract_comments.log", level=logging.INFO)
logger = logging.getLogger(__name__)

while True:
    comments_to_extract = cursor.execute(
        """
        SELECT id, body
        FROM comment
        WHERE
            triaged_at_utc IS NOT NULL AND
            (extracted_at_utc IS NULL OR extraction_model != ?)
        LIMIT 16
        """,
        (llm_model,),
    ).fetchall()

    if not comments_to_extract:
        break

    system_prompt = """
You are an assistant that extracts structured data from Reddit comments.

Return ONLY valid JSON.

Output format:
[
    {
        "id": string,
        "brand": string (product brand),
        "model": string (product model),
        "category": string (e.g. "keyboard", "mouse"),
        "context": string (e.g. "office", "gaming"),
        "attributes": JSON object (e.g. "{"layout_size": "60%", "switch_type": "mechanical"}"),
        "price": number,
        "currency": string (e.g. "USD", "AUD", assume USD if not specified),
        "sentiment": string ("positive"/"neutral"/"negative"),
        "positives": JSON array of strings (short phrases),
        "negatives": JSON array of strings (short phrases),
        "miscellaneous": JSON array of strings (short phrases)
    }
]

Example:

Input: "I bought the Keychron K2, and it's amazing! The mechanical keys feel great for programming and I like the design, but I wish the backlight was brighter and it was a bit lighter. It took three weeks to ship and comes with a carrying case. Paid $99 USD. Highly recommend it for office work."

Output:
[
    {
        "id": "abc123",
        "brand": "Keychron",
        "model": "K2",
        "category": "keyboard",
        "context": "office",
        "attributes": {"layout_size": "75%", "switch_type": "mechanical"},
        "price": 99,
        "currency": "USD",
        "sentiment": "positive",
        "positives": ["Mechanical keys feel great for programming.", "I like the design."],
        "negatives": ["Backlight was not bright enough.", "It was a bit heavy."],
        "miscellaneous": ["Took three weeks to ship.", "Comes with a carrying case."]
    }
]

Rules:
- Use the exact ID from each comment
- Do not omit any comments
- Do not include any text outside JSON
- If a field is not present, return null
- Always return valid JSON with these fields
- Use JSON format exactly as specified
- For lists, use an array in JSON
- For attributes, use an object in JSON
"""

    comments_text = ""

    for comment in comments_to_extract:
        (id, body) = comment
        comments_text += f"""\n\n<COMMENT id="{id}">\n{body}\n</COMMENT>"""

    response = llm.create_chat_completion(
        messages=[
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": f"Now extract the same fields for the following comment:{comments_text}",
            },
        ],
    )["choices"][0]["message"]["content"]

    extracted_comments = json.loads(response)

    for comment in extracted_comments:
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
        logger.info(f"Extracted comment id={id}")

connection.close()
