import os
from datetime import datetime
from typing import Any
import json
from src.llm_reviewer.notebook_reviewer import review_notebook
from src.llm_reviewer.constants import PROJECT_ROOT


def highlight_text_in_red(text: str, target: str) -> str:
    """
    Given a piece of text, find the target in the string, make it red in markdown and return the modified string.

    :param text: The original text.
    :param target: The target string to be highlighted.
    :return: The modified text with the target string highlighted in red.
    """
    if f"<span style='color:red'>{target}</span>" in text:
        return text
    else:
        return text.replace(target, f"<span style='color:red'>{target}</span>")


def create_report_folder(
    ipynb_filename: str, include_parent_folder: bool = False
) -> str:
    """
    Get the path to the report folder, creating it if necessary.

    :param ipynb_filename: The filename of the notebook.
    :param include_parent_folder: Whether to include the parent folder in the path.
    :return: The path to the report folder.
    """
    folder_path = os.path.join(PROJECT_ROOT, "reports")
    if include_parent_folder:
        parent_folder = os.path.basename(os.path.dirname(ipynb_filename))
        folder_path = os.path.join(folder_path, parent_folder)

    counter = 1
    original_folder_path = folder_path
    while os.path.exists(folder_path):
        folder_path = f"{original_folder_path}_{counter}"
        counter += 1

    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    return folder_path


def generate_report(
    review: dict[str, list[dict[str, Any]]],
    ipynb_filename: str  = None,
    save_folder_path: str  = None,
) -> str:
    """
    Generate a markdown formatted review report.

    :param results: The review results.
    :param save_to_file: Whether to save the report to a file.
    :return: The report as a string.
    """
    report_filename_no_ext = None
    if save_folder_path:
        timestamp = datetime.now().strftime("%Y-%m-%d__%H_%M_%S")
        ipynb_filename_stripped = os.path.splitext(os.path.basename(ipynb_filename))[0][
            :20
        ]
        folder_path = os.path.join(
            save_folder_path, f"{ipynb_filename_stripped}__{timestamp}"
        )

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        report_filename_no_ext = os.path.join(folder_path, "report")
        with open(report_filename_no_ext + ".json", "w") as f:
            json.dump(review, f, indent=4)
    turns_reports = []

    for i, result in enumerate(review["turns"]):
        report = []
        report.append(f"## Turn {i+1}")
        report.append("\n")
        for step in result["turn"]:
            if step["role"] == "human":
                report.append("### Human")
            else:
                report.append("### LLM")
            report.append("\n")
            for content in step["steps"]:
                if content["type"] == "markdown":
                    report.append(content["content"])
                    report.append("\n")
                elif content["type"] == "code":
                    report.append("```python")
                    report.append(content["content"])
                    report.append("```")
                    report.append("\n")
        report = ["\n".join(report)]
        for reviewer in ["english_review", "code_review"]:
            report.append(f"### {reviewer.replace('_', ' ').title()}")
            report.append("\n")
            report.append("#### Score: " + str(result[reviewer]["score"]))
            report.append("### Feedback Text")
            report.append("\n")
            report.append(result[reviewer]["feedback_text"].replace("\n", "\n\n"))
        turns_reports.append("\n".join(report))

    full_report_str = "\n".join(turns_reports)

    if save_folder_path:
        with open(report_filename_no_ext + ".md", "w") as f:
            f.write(full_report_str)
    print(f"Report generated at: {report_filename_no_ext}.md")
    return full_report_str
