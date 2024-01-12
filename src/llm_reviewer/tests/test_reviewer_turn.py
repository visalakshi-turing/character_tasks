import pytest
from llm_reviewer.turn_reviewer import review_turn
from llm_reviewer.turn_reviewer import load_schema
from jsonschema import validate, ValidationError


def test_review_turn_no_errors(llm_client, llm_configs):
    texts = {"human": "dummy human text", "llm": "dummy llm text"}
    reviewer_name = "english_reviewer"
    schema = load_schema(reviewer_name)

    try:
        result = review_turn(
            reviewer_name, texts, llm_client, llm_configs["english_reviewer"]
        )
        print(result)
        validate(instance=result, schema=schema)
    except ValidationError as ve:
        pytest.fail(f"review_turn result does not match the schema: {ve}")
    except Exception as e:
        pytest.fail(f"review_turn raised an exception: {e}")
    assert result != schema, "The result should not be equal to the schema itself"
