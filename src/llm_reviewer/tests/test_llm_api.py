import pytest
from llm_reviewer.llm_api import make_llm_request


def test_wrong_response_format(llm_client, llm_configs):
    with pytest.raises(ValueError):
        make_llm_request(
            llm_client,
            [],
            **{
                **llm_configs["english_reviewer"],
                "response_format": "unsupported_format",
            },
        )


def test_response_format_none(llm_client, llm_configs):
    response = make_llm_request(
        llm_client,
        [{"role": "system", "content": 'reply "Hello@@@" and nothing else->'}],
        **{
            **llm_configs["english_reviewer"],
            "response_format": None,
        },
    )
    assert "Hello@@@" in response


def test_response_format_json_object(llm_client, llm_configs):
    response = make_llm_request(
        llm_client,
        [
            {
                "role": "system",
                "content": "reply with JSON format first 3 prime_numbers and nothing else->",
            }
        ],
        **{
            **llm_configs["english_reviewer"],
            "response_format": {"type": "json_object"},
        },
    )
    assert response == {"prime_numbers": [2, 3, 5]}
