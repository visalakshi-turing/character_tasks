import os

import pytest
from llm_reviewer.notebook_parser import parse_notebook


def test_parse_notebook_turn_count(notebook_samples):
    for path in notebook_samples:
        try:
            result = parse_notebook(path)
        except Exception as e:
            pytest.fail(f"Error parsing notebook {path}: {str(e)}")

        expected_turn_count = int(os.path.basename(path).split("T__")[0])
        assert (
            len(result) == expected_turn_count
        ), f"Expected {expected_turn_count} turns, but got {len(result)} in {path}"
