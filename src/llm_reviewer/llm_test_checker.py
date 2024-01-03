import os
import json
from src.llm_reviewer.llm_api import make_llm_request
from src.llm_reviewer.constants import PROJECT_CODEBASE


def test_llm_based_output(
    llm_output: str,
    expected_output: str,
    task_context: str,
    check_rules: str,
    llm_client,
    llm_config: dict,
    retries: int = 2,
) -> dict:
    """
    Test the LLM output using the given parameters.

    :param llm_output: The output from the LLM.
    :param expected_output: The expected output.
    :param task_context: The context for the test and the task.
    :param check_rules: The rules to check the test against.
    :param llm_client: The LLM client to use for making requests.
    :param llm_config: The configuration for the LLM client.
    :param retries: The number of retries if the test fails.

    :return: A JSON object with the status of the test and the reason if it failed.
    """
    prompt_path = os.path.join(PROJECT_CODEBASE, "prompts", "tester.txt")
    with open(prompt_path, "r", encoding="utf-8") as file:
        prompt = file.read()

    prompt = (
        prompt.replace("@LLM_OUTPUT@", llm_output)
        .replace("@EXPECTED_OUTPUT@", expected_output)
        .replace("@TASK_CONTEXT@", task_context)
        .replace("@CHECK_RULES@", check_rules)
    )

    result = None
    messages = [{"role": "system", "content": prompt}]
    for i in range(retries):
        result = make_llm_request(client=llm_client, messages=messages, **llm_config)

        if (
            "status" in result
            and "reason" in result
            and result["status"] in ["pass", "fail"]
        ):
            return result
        elif i < retries - 1:
            print(
                f"Attempt {i + 1} failed. Retrying... {retries - i - 1} attempts left."
            )
        else:
            print(
                f"All {retries} attempts failed. Unable to get a valid response from the LLM tester."
            )

    return {
        "status": "fail",
        "reason": f"LLM tester response is missing or has invalid 'status' or 'reason' field. Provided response:\n{json.dumps(result, indent=4)}",
    }
