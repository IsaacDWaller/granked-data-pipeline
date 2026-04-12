import os


def get_logging_filename(file_path):
    folder_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs"
    )

    os.makedirs(folder_path, exist_ok=True)
    return os.path.join(folder_path, file_path)
