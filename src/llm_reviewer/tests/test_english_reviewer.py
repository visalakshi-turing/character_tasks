import pytest
from llm_reviewer.turn_reviewer import review_turn
from llm_reviewer.turn_reviewer import load_schema
from jsonschema import validate, ValidationError


english_test_cases = [
    {
        "human_text": "What is the weather today?",
        "llm_text": "Weather is good is.",
        "reason": "Correctness: The answer is not correct. The sentence 'Weather is good is.' is grammatically incorrect. Precision: The answer is not precise. It does not provide specific weather conditions. Conciseness: The answer is not concise due to the repetition of 'is'. Clarity: The answer is not clear due to the grammatical error.",
        "must_contain": ["correctness", "precision", "conciseness", "clarity"],
    },
    {
        "human_text": "What is the time complexity of quicksort?",
        "llm_text": "The time complexity of quicksort is O(n log n) in the best case and O(n^2) in the worst case.",
        "reason": "Correctness: The answer is correct. Precision: The answer is not precise enough. It should mention that the average case is also O(n log n). Conciseness: The answer is concise. Clarity: The answer is clear.",
        "must_contain": ["precision"],
    },
    {
        "human_text": "What is the capital of France?",
        "llm_text": "The capital of France is the city known as Paris.",
        "reason": "Correctness: The answer is correct. Precision: The answer is precise. Conciseness: The answer is not concise. It could simply be 'Paris'. Clarity: The answer is clear.",
        "must_contain": ["conciseness"],
    },
    {
        "human_text": "What is the chemical formula for water?",
        "llm_text": "The chemical formula for water is H2O, which means there are two hydrogen atoms and one oxygen atom.",
        "reason": "Correctness: The answer is correct. Precision: The answer is precise. Conciseness: The answer is not concise. The additional explanation is unnecessary. Clarity: The answer is clear.",
        "must_contain": ["conciseness"],
    },
    {
        "human_text": "What is the square root of 16?",
        "llm_text": "The square root of 16 is 4, because 4 times 4 equals 16.",
        "reason": "Correctness: The answer is correct. Precision: The answer is precise. Conciseness: The answer is not concise. The explanation is unnecessary. Clarity: The answer is clear.",
        "must_contain": ["conciseness"],
    },
    {
        "human_text": "What is the distance from Earth to the Moon?",
        "llm_text": "The distance from Earth to the Moon is approximately 238,855 miles, but this can vary slightly due to the Moon's elliptical orbit.",
        "reason": "Correctness: The answer is correct. Precision: The answer is not precise. The variation in distance is not quantified. Conciseness: The answer is concise. Clarity: The answer is clear.",
        "must_contain": ["precision"],
    },
    {
        "human_text": "What is the speed of light?",
        "llm_text": "The speed of light is approximately 299,792 kilometers per second, which is the maximum speed at which all conventional matter and hence all known forms of information in the universe can travel.",
        "reason": "Correctness: The answer is correct. Precision: The answer is precise. Conciseness: The answer is not concise. The additional information is unnecessary. Clarity: The answer is clear.",
        "must_contain": ["conciseness"],
    },
    {
        "human_text": "What is the boiling point of water?",
        "llm_text": "The boiling point of water is 100 degrees Celsius at sea level, but this can vary depending on the atmospheric pressure.",
        "reason": "Correctness: The answer is correct. Precision: The answer is not precise. It should quantify how much the boiling point can vary. Conciseness: The answer is concise. Clarity: The answer is clear.",
        "must_contain": ["precision"],
    },
    {
        "human_text": "What is the atomic number of hydrogen?",
        "llm_text": "The atomic number of hydrogen is 1, which means it has one proton in its nucleus.",
        "reason": "Correctness: The answer is correct. Precision: The answer is precise. Conciseness: The answer is not concise. The explanation is unnecessary. Clarity: The answer is clear.",
        "must_contain": ["conciseness"],
    },
    {
        "human_text": "What is the Pythagorean theorem?",
        "llm_text": "The Pythagorean theorem states that in a right-angled triangle, the square of the hypotenuse (the side opposite the right angle) is equal to the sum of the squares of the other two sides. This can be written as: a^2 + b^2 = c^2.",
        "reason": "Correctness: The answer is correct. Precision: The answer is not precise. It should mention that this only applies to right-angled triangles. Conciseness: The answer is concise. Clarity: The answer is clear.",
        "must_contain": ["precision"],
    },
    {
        "human_text": "What is the law of universal gravitation?",
        "llm_text": "The law of universal gravitation states that every particle of matter in the universe attracts every other particle with a force that is directly proportional to the product of their masses and inversely proportional to the square of the distance between their centers.",
        "reason": "Correctness: The answer is correct. Precision: The answer is not precise. It should mention that this is an approximation and does not hold under certain conditions (e.g., at very small scales). Conciseness: The answer is concise. Clarity: The answer is clear.",
        "must_contain": ["precision"],
    },
    {
        "human_text": "What is the law of universal gravitation?",
        "llm_text": "The law of universal gravitation states that every particle of matter in the universe attracts every other particle with a force that is directly proportional to the product of their masses and inversely proportional to the square of the distance between their centers.",
        "reason": "Correctness: The answer has a grammatical error in the phrase 'every particle of matter in the universe attracts every other particle'. Precision: The answer is precise. Conciseness: The answer is concise. Clarity: The answer is clear.",
        "must_contain": ["correctness"],
    },
    {
        "human_text": "What is the speed of light?",
        "llm_text": "The speed of light is approximately 299,792 kilometers per second, which is the maximum speed at which all conventional matter and hence all known forms of information in the universe can travel.",
        "reason": "Correctness: The answer has a grammatical error in the phrase 'which is the maximum speed at which all conventional matter and hence all known forms of information in the universe can travel'. Precision: The answer is precise. Conciseness: The answer is concise. Clarity: The answer is clear.",
        "must_contain": ["correctness"],
    },
    {
        "human_text": "What is the boiling point of water?",
        "llm_text": "The boiling point of water is 100 degrees Celsius at sea level, but this can vary depending on the atmospheric pressure.",
        "reason": "Correctness: The answer has a grammatical error in the phrase 'but this can vary depending on the atmospheric pressure'. Precision: The answer is precise. Conciseness: The answer is concise. Clarity: The answer is clear.",
        "must_contain": ["correctness"],
    },
    {
        "human_text": "What is the law of universal gravitation?",
        "llm_text": "It's about how stuff in the universe pulls on other stuff. It's got to do with mass and distance and stuff.",
        "reason": "Correctness: The answer is correct. Precision: The answer is precise. Conciseness: The answer is concise. Clarity: The answer is not clear.",
        "must_contain": ["clarity"],
    },
    {
        "human_text": "What is the Pythagorean theorem?",
        "llm_text": "It's a thing with triangles, you know, the one with the squares and the sides and stuff.",
        "reason": "Correctness: The answer is correct. Precision: The answer is precise. Conciseness: The answer is concise. Clarity: The answer is not clear.",
        "must_contain": ["clarity"],
    },
    {
        "human_text": "What is the speed of light?",
        "llm_text": "It's super fast, like, the fastest thing ever. Nothing can go faster, you know?",
        "reason": "Correctness: The answer is correct. Precision: The answer is precise. Conciseness: The answer is concise. Clarity: The answer is not clear.",
        "must_contain": ["clarity"],
    },
]


@pytest.mark.parametrize("test_case", english_test_cases)
def test_english_test_cases(llm_client, llm_configs, test_case):
    reviewer_name = "english_reviewer"
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


"""
def test_review_turn_bad_grammar(llm_client, llm_configs):
    texts = {"human": "What is the weather today?", "llm": "Weather is good is."}
    reviewer_name = "english_reviewer"
    schema = load_schema(reviewer_name)

    for i in range(5):
        try:
            result = review_turn(
                reviewer_name, texts, llm_client, llm_configs["english_reviewer"]
            )
            print(result)
            validate(instance=result, schema=schema)
        except ValidationError as ve:
            print(result)
            pytest.fail(f"review_turn result does not match the schema: {ve}")
        except Exception as e:
            print(result)
            pytest.fail(f"review_turn raised an exception: {e}")
        assert result != schema, "The result should not be equal to the schema itself"
        assert (
            "correctness" in result
        ), f"The result should contain 'correctness' key. Full result: {result}"
        assert result["correctness"], "The result should contain grammar issues"
"""
