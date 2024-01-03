import json


def load_config(config_file_path: str) -> dict[str, str]:
    with open(config_file_path) as config_file:
        config = json.load(config_file)
    return config
