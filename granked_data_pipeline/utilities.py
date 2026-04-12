import json
import os
import re


def get_logging_filename(file_path):
    folder_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs"
    )

    os.makedirs(folder_path, exist_ok=True)
    return os.path.join(folder_path, file_path)


def get_json_match(string):
    match: re.Match | None = re.search(r"\[\s*{.*?}\s*\]", string, re.DOTALL)

    if not match:
        return None

    return json.loads(match.group())
