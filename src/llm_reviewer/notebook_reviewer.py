import traceback
from typing import Any
from src.llm_reviewer.notebook_parser import notebook_to_turns
from src.llm_reviewer.turn_reviewer import review_turn
from src.llm_reviewer.llm_api import load_config, LLMAPIFactory
from src.llm_reviewer.constants import PATH_TO_CONFIG, PATH_TO_SECRETS, Roles
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import pandas as pd

from enum import Enum


class IssueLevel(Enum):
    MINOR = 1
    MEDIUM = 2
    CRITICAL = 3


STR_TO_ISSUE_LEVEL = {
    "minor_issues": IssueLevel.MINOR,
    "medium_issues": IssueLevel.MEDIUM,
    "critical_issues": IssueLevel.CRITICAL,
}


def format_turn_data(turn: dict[str, Any]) -> dict[str, str]:
    """
    Format the turn data for review.

    :param turn: A dictionary representing a turn.
    :return: A formatted dictionary ready for review.
    """
    formatted_turn = ""
    for role_turn in turn:
        if role_turn["role"] == Roles.HUMAN.value:
            formatted_turn += "# HUMAN_REPLY_START\n"
        elif role_turn["role"] == Roles.LLM.value:
            formatted_turn += "# LLM_REPLY_START\n"
        else:
            raise ValueError(f"Unexpected role: {role_turn['role']}")
        for step in role_turn["steps"]:
            if step["type"] == "markdown":
                formatted_turn += step["content"] + "\n"
            elif step["type"] == "code":
                formatted_turn += "```\n" + step["content"] + "\n```\n"
            else:
                raise ValueError(f"Unexpected step type: {step['type']}")
        if role_turn["role"] == Roles.HUMAN.value:
            formatted_turn += "# HUMAN_REPLY_END\n\n"
        elif role_turn["role"] == Roles.LLM.value:
            formatted_turn += "# LLM_REPLY_END\n\n"
    return formatted_turn


def turn_reviewer_worker(
    turn_review_queue: Queue,
    config: dict[str, Any],
    results: list[dict],
    total_reviews: int,
) -> None:
    try:
        llm_client = LLMAPIFactory(PATH_TO_SECRETS).get()
        while not turn_review_queue.empty():
            turn_id = None
            reviewer = None
            reviews_left = 0
            try:
                reviews_done = len(results)
                reviews_left = turn_review_queue.qsize() - 1
                print(
                    f"Reviews done: {reviews_done}, Reviews left after this one: {reviews_left}"
                )
                turn_id, reviewer, turn = turn_review_queue.get()
                r = review_turn(
                    reviewer,
                    format_turn_data(turn),
                    llm_client,
                    config[reviewer],
                )
                results.append({"id": turn_id, "reviewer": reviewer, "result": r})
            except Exception as e:
                msg = f"An error occurred while processing {turn_id=} for {reviewer=}: {e}"
                print(msg)
                results.append({"id": turn_id, "reviewer": reviewer, "result": "ERROR"})
            finally:
                turn_review_queue.task_done()
                print(
                    f"Review for {turn_id=} by {reviewer=} is done. {len(results)} / {total_reviews} reviews completed."
                )
    except Exception as e:
        traceback.print_exc()
        return None


def populate_queue(queue: Queue, turns: list) -> None:
    for i, turn in enumerate(turns):
        queue.put((i, "english_reviewer", turn))
        queue.put((i, "code_reviewer", turn))


def review_notebook(notebook, max_threads=1) -> dict[str, list[dict[str, Any]]]:
    """
    Review a notebook file and return a list of dictionaries, each representing a review result.
    Each dictionary in the list has two keys: 'turn' and 'review'.
    'turn' is a dictionary with keys 'human' and 'llm', representing the human and LLM assistant parts of the turn.
    'review' is the result of the LLM review for the LLM Assistant part of the turn.

    :param nb_path: The path to the notebook file.
    :return: A list of dictionaries, each representing the review result of a turn.
    """
    try:
        config = load_config(PATH_TO_CONFIG)

        turns = notebook_to_turns(notebook["nb_parsed_notebook"])

        turn_queue = Queue()
        populate_queue(turn_queue, turns)
        total_turns = len(turns)
        results = []
        max_threads = min(total_turns * 2, max_threads)
        if max_threads == 0:
            print(f"Review process completed UNsuccessfully. {notebook['file_id']}")
            return None
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            for _ in range(max_threads):
                executor.submit(
                    turn_reviewer_worker, turn_queue, config, results, total_turns * 2
                )
        turn_queue.join()

        gathered_results = [{"turn": turn} for turn in turns]
        for result in results:
            turn_id, reviewer, review_result = (
                result["id"],
                result["reviewer"],
                result["result"],
            )
            if reviewer == "english_reviewer":
                gathered_results[turn_id]["english_review"] = review_result
            elif reviewer == "code_reviewer":
                gathered_results[turn_id]["code_review"] = review_result
            else:
                raise Exception(f"Unknown reviewer: {reviewer}")

        print("Review process completed successfully.")

        return {"turns": gathered_results, "nb_path": notebook["file_id"]}
    except Exception as e:
        print(f"Review process completed UNsuccessfully. {notebook['file_id']}")
        traceback.print_exc()
        return None


def review_notebooks(notebooks, max_threads_per_notebook=1, max_concurrent_notebooks=1):
    """
    Review multiple notebook files in parallel and return a list of review results for each notebook.

    :param notebooks_paths: A list of paths to the notebook files.
    :param max_threads_per_notebook: Maximum number of threads to use for reviewing each notebook.
    :param max_concurrent_notebooks: Maximum number of notebooks to review concurrently.
    :return: A list of lists, each containing dictionaries of review results for a notebook.
    """
    max_concurrent_notebooks = min(len(notebooks), max_concurrent_notebooks)
    if not notebooks:
        return []
    with ThreadPoolExecutor(max_workers=max_concurrent_notebooks) as executor:
        results = executor.map(
            lambda nb_path: review_notebook(
                nb_path, max_threads=max_threads_per_notebook
            ),
            notebooks,
        )
    return list(results)


def review_to_row(review, issue_level=None):
    """
    Convert a review dictionary into a row that can be added to a DataFrame, aggregating scores and feedback.

    :param review: A dictionary containing review details for a notebook.
    :return: A dictionary representing the row to be added to the DataFrame.
    """
    nb_path = review.get("nb_path", "")
    turns = review.get("turns", [])

    # Initialize scores and feedback
    code_scores = []
    lang_scores = []
    combined_feedback = []
    combined_code_feedback = []
    combined_lang_feedback = []

    # Process each turn and aggregate scores and feedback
    for i, turn in enumerate(turns):
        english_review = turn.get("english_review")
        code_review = turn.get("code_review")

        # Append scores or handle errors
        lang_score = (
            english_review["score"] if isinstance(english_review, dict) else "ERROR"
        )
        code_score = code_review["score"] if isinstance(code_review, dict) else "ERROR"
        if lang_score != "ERROR":
            lang_scores.append(lang_score)
        if code_score != "ERROR":
            code_scores.append(code_score)

        # Combine feedback or show ERROR
        english_feedback = (
            english_review.get("feedback_text", "ERROR")
            if isinstance(english_review, dict)
            else "ERROR"
        )
        code_feedback = (
            code_review.get("feedback_text", "ERROR")
            if isinstance(code_review, dict)
            else "ERROR"
        )

        # Process feedback_text dict into markdown formatted sections
        if isinstance(english_feedback, dict):
            english_feedback_lines = []
            for k, v in english_feedback.items():
                try:
                    if v.strip() and k in STR_TO_ISSUE_LEVEL and STR_TO_ISSUE_LEVEL[k].value >= issue_level.value:
                        english_feedback_lines.append(f"**{k.title()}**\n{v if v.strip() else 'None'}")
                except Exception as e:
                    print(k, v)
            english_feedback = "\n".join(english_feedback_lines)
        elif issue_level is not None:
            raise Exception("Issue level is supported with dict issues only.")
        if isinstance(code_feedback, dict):
            code_feedback_lines = []
            for k, v in code_feedback.items():
                try:
                    if v.strip() and k in STR_TO_ISSUE_LEVEL and STR_TO_ISSUE_LEVEL[k].value >= issue_level.value:
                        code_feedback_lines.append(f"**{k.title()}**\n{v if v.strip() else 'None'}")
                except Exception as e:
                    print(k, v)
            code_feedback = "\n".join(code_feedback_lines)
        elif issue_level is not None:
            raise Exception("Issue level is supported with dict issues only.")

        if not english_feedback.strip():
            english_feedback = "None"
        if not code_feedback.strip():
            code_feedback = "None"

        combined_feedback.append(
            f"#Turn {i+1}:\n\n## Language({lang_score}/5):\n{english_feedback}\n\n## Code({code_score}/5):\n{code_feedback}"
        )
        combined_code_feedback.append(
            f"#Turn {i+1}:\n\n## Code({code_score}/5):\n{code_feedback}"
        )
        combined_lang_feedback.append(
            f"#Turn {i+1}:\n\n## Language({lang_score}/5):\n{english_feedback}"
        )

    # Calculate average scores
    avg_code_score = sum(code_scores) / len(code_scores) if code_scores else None
    avg_lang_score = sum(lang_scores) / len(lang_scores) if lang_scores else None
    combined_feedback = "\n\n======\n\n".join(combined_feedback)
    return {
        "nb_path": nb_path,
        "code_score": avg_code_score,
        "lang_score": avg_lang_score,
        "comb_feedback": combined_feedback,
        "code_feedback": "\n\n======\n\n".join(combined_code_feedback),
        "lang_feedback": "\n\n======\n\n".join(combined_lang_feedback),
    }


def notebook_reviews_to_df(reviews: list, issue_level=None):
    df = pd.DataFrame(
        list(map(lambda x: review_to_row(x, issue_level), reviews)),
        columns=[
            "nb_path",
            "code_score",
            "lang_score",
            "comb_feedback",
            "code_feedback",
            "lang_feedback",
        ],
    )
    return df
