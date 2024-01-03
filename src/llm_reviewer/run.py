import argparse
import os
import traceback
from datetime import datetime

from src.llm_reviewer.notebook_reviewer import notebook_reviews_to_df, review_notebooks
from src.llm_reviewer.reports import (
    create_report_folder,
    generate_report,
    review_notebook,
)


def load_notebooks_from_folder(nb_folder: str):
    return [
        os.path.join(nb_folder, name)
        for name in os.listdir(nb_folder)
        if name.endswith(".ipynb")
    ]


def main():
    parser = argparse.ArgumentParser(
        description="Generate a report from a .ipynb file."
    )
    parser.add_argument(
        "path",
        type=str,
        help="Path to the .ipynb file or directory containing .ipynb files.",
    )
    parser.add_argument(
        "--max_threads_per_notebook",
        type=int,
        default=3,
        help="Maximum number of threads to use.",
    )
    parser.add_argument(
        "--max_concurrent_notebooks",
        type=int,
        default=1,
        help="Maximum number of concurrent notebooks to review.",
    )
    args = parser.parse_args()
    max_threads_per_notebook = args.max_threads_per_notebook
    if os.path.isdir(args.path):
        notebook_files = load_notebooks_from_folder(args.path)
        if not notebook_files:
            print("No notebooks found in the specified directory.")
            return
        try:
            results = review_notebooks(
                notebooks_paths=notebook_files,
                max_threads_per_notebook=max_threads_per_notebook,
                max_concurrent_notebooks=args.max_concurrent_notebooks,
            )
            report_folder = create_report_folder(
                notebook_files[0], include_parent_folder=True
            )
            for result, notebook_path in zip(results, notebook_files):
                generate_report(result, notebook_path, save_folder_path=report_folder)
            # Dump the review results as a CSV file
            df = notebook_reviews_to_df(results)
            timestamp = datetime.now().strftime("%Y-%m-%d__%H_%M_%S")
            df.to_csv(os.path.join(report_folder, f"report_{timestamp}.csv"))
        except Exception as e:
            print(f"Error processing notebooks in {args.path}: {str(e)}")
            traceback.print_exc()
    elif args.path.endswith(".ipynb"):
        result = review_notebook(args.path, max_threads=max_threads_per_notebook)

        report_folder = create_report_folder(args.path, include_parent_folder=True)
        generate_report(result, args.path, save_folder_path=report_folder)
    else:
        print(
            "Invalid path. Please provide a path to a .ipynb file or a directory containing .ipynb files."
        )


if __name__ == "__main__":
    main()
