import pytest
from llm_reviewer.turn_reviewer import review_turn
from llm_reviewer.turn_reviewer import load_schema
from jsonschema import validate, ValidationError


code_test_cases = [
    {
        "human_text": "Refactor this code for better readability: `def f(x): return x+1`",
        "llm_text": "```python\ndef f(x):\n    return x + 1\n```",
        "reason": "The refactored code is not more readable, the function name and parameter are not descriptive.",
        "must_contain": ["code_quality_problems"],
    },
    {
        "human_text": "Is this function implementation following best practices? `def add(a, b): return a + b`",
        "llm_text": "```python\ndef add(a, b):\n    return a + b\n```",
        "reason": "The function correctly adds two numbers, but it lacks type annotations which are considered a best practice for clarity and type checking.",
        "must_contain": ["failure_to_follow_best_practices"],
    },
    {
        "human_text": "Find the bug in this code: `def get_even(numbers): return [n for n in numbers if n%2]`",
        "llm_text": "```python\ndef get_even(numbers):\n    return [n for n in numbers if n % 2]\n```",
        "reason": "The code returns odd numbers instead of even numbers. The condition `n % 2` should be `n % 2 == 0`.",
        "must_contain": ["bugs_found"],
    },
    {
        "human_text": "Explain the purpose of this code: `def greet(name): print(f'Hello, {name}')`",
        "llm_text": "```python\ndef greet(name):\n    print(f'Hello, {name}')\n```",
        "reason": "The explanation does not mention that f-strings are a Python 3.6+ feature.",
        "must_contain": ["ease_of_understanding_fails"],
    },
    {
        "human_text": "Improve this code's quality: `def sum_list(lst): return sum(lst)`",
        "llm_text": "```python\ndef sum_list(lst):\n    return sum(lst)\n```",
        "reason": "The code is not improved, the function name is not descriptive and it lacks type annotations.",
        "must_contain": ["code_quality_problems"],
    },
    {
        "human_text": "Is this function signature following best practices? `def subtract(x, y): return x - y`",
        "llm_text": "```python\ndef subtract(x, y):\n    return x - y\n```",
        "reason": "The function performs subtraction, but it lacks type annotations which are considered a best practice for clarity and type checking.",
        "must_contain": ["failure_to_follow_best_practices"],
    },
    {
        "human_text": "Identify and fix the bug in this code: `def is_prime(n): for i in range(2, n): if n % i == 0: return False return True`",
        "llm_text": "```python\ndef is_prime(n):\n    for i in range(2, n):\n        if n % i == 0:\n            return False\n    return True\n```",
        "reason": "The code does not handle the case where n is less than 2, and it does not use the optimized approach of checking up to the square root of n.",
        "must_contain": ["bugs_found"],
    },
    {
        "human_text": "Clarify the functionality of this code: `def double(n): return 2 * n`",
        "llm_text": "```python\ndef double(n):\n    return 2 * n\n```",
        "reason": "The explanation does not use type annotations to clarify the expected input and output types.",
        "must_contain": ["ease_of_understanding_fails"],
    },
    {
        "human_text": "Enhance the quality of this code: `def max_value(numbers): return max(numbers)`",
        "llm_text": "```python\ndef max_value(numbers):\n    return max(numbers)\n```",
        "reason": "The code is not enhanced, the function name is not descriptive and it lacks type annotations.",
        "must_contain": ["code_quality_problems"],
    },
    {
        "human_text": "Does this function adhere to best coding practices? `def power(base, exponent): return base ** exponent`",
        "llm_text": "```python\ndef power(base, exponent):\n    return base ** exponent\n```",
        "reason": "The function calculates the power correctly, but it lacks type annotations which are considered a best practice for clarity and type checking.",
        "must_contain": ["failure_to_follow_best_practices"],
    },
    {
        "human_text": "Debug this code: `def factorial(n): return 1 if n == 0 else n * factorial(n)`",
        "llm_text": "```python\ndef factorial(n):\n    return 1 if n == 0 else n * factorial(n)\n```",
        "reason": "The code has a bug in the recursive call; it should pass `n - 1` instead of `n` to avoid infinite recursion.",
        "must_contain": ["bugs_found"],
    },
    {
        "human_text": "Describe the purpose of this code: `def say_hello(): print('Hello!')`",
        "llm_text": "```python\ndef say_hello():\n    print('Hello!')\n```",
        "reason": "The explanation does not include a return type annotation to clarify that the function does not return any value.",
        "must_contain": ["ease_of_understanding_fails"],
    },
    {
        "human_text": "Please format this code correctly: `x=  { 'a':37,'b':42, 'c':927}`",
        "llm_text": "```python\nx={'a':37,'b':42,'c':927}\n```",
        "reason": "The LLM has formatted the code, but it has not maintained a consistent coding style with previous examples in terms of spacing around the assignment operator and within the dictionary.",
        "must_contain": ["consistency_of_coding_style_for_same_person_fails"],
    },
    {
        "human_text": "Can you convert this list of numbers to a list of strings? `[1, 2, 3]`",
        "llm_text": "```python\nstr_numbers=list(map(str, [1,2,3]))\n```",
        "reason": "The LLM correctly converts the numbers to strings, but uses a different variable naming style and inconsistent spacing than previously used, breaking consistency.",
        "must_contain": ["consistency_of_coding_style_for_same_person_fails"],
    },
    {
        "human_text": "Optimize this code for better performance: `doubles = [x * 2 for x in range(10)]`",
        "llm_text": "```python\ndoubles=(x*2 for x in range(10))\n```",
        "reason": "The LLM has optimized the code by using a generator expression, but the use of parentheses instead of brackets and inconsistent spacing is inconsistent with the list comprehensions used in previous examples.",
        "must_contain": ["consistency_of_coding_style_for_same_person_fails"],
    },
]


@pytest.mark.parametrize("test_case", code_test_cases)
def test_code_test_cases(llm_client, llm_configs, test_case):
    reviewer_name = "code_reviewer"
    schema = load_schema(reviewer_name)

    texts = {"human": test_case["human_text"], "llm": test_case["llm_text"]}
    print(f"Processing test case '{texts}'")
    try:
        result = review_turn(
            reviewer_name, texts, llm_client, llm_configs[reviewer_name]
        )
        print("=" * 60)
        print(test_case)
        print(result)
        validate(instance=result, schema=schema)
    except ValidationError as ve:
        print(result)
        pytest.fail(f"review_turn result does not match the schema: {ve}")
    except Exception as e:
        print(result)
        pytest.fail(f"review_turn raised an exception: {e}")
    assert result != schema, "The result should not be equal to the schema itself"
    for key in test_case["must_contain"]:
        assert (
            key in result
        ), f"The result should contain '{key}' key. Full result: {result}"
        assert result[key], f"{key} must be present"
