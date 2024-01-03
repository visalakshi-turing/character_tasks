# WELCOME

## Usage

This project is used to generate a report from a .ipynb file.

To run the program, use the following command:

```python
python llm_reviewer/run.py [path] --max_threads_per_notebook [number] --max_concurrent_notebooks [number]
```

Where:

- `[path]` is the path to the .ipynb file or directory containing .ipynb files.
- `[number]` for `--max_threads_per_notebook` is the maximum number of threads to use per notebook (default is 3).
- `[number]` for `--max_concurrent_notebooks` is the maximum number of notebooks to review concurrently (default is 1).

Note: Maximum number of requests to the GPT at the same time would be max_concurrent_notebooks\*max_threads_per_notebook. No time based rate limiting is conducted.

If the path is a directory, the program will process all .ipynb files in the directory concurrently according to the specified maximums.
For each file, it will print the start and end of processing, and any errors encountered.

If the path is a .ipynb file, the program will process the file and generate a report.

If the path is neither a directory nor a .ipynb file, the program will print an error message.

Generated reports will be placed into PROJECT_ROOT/reports/ folder. If path is a folder, a folder with the same name will be created in the reports. If this folder is processed multiple times - the counter will be added to it.

# A little bit about what it does:

This project uses OpenAI GPT API to generate reviews for AI conversation trajectories provided in the form of Jupyter Notebook files - .ipynb format.

Reviews are provided **independently** for each turn in 2 categories - English and Code quality - but only for LLM role in the turn. Human role input is used only for context.

Each review for each turn in each category consists of `feedback_text` with pointers about the issues and `score` from 1 to 5.

Turn is considered to be a list of maximum 2 items - where each item is a sequence of consequitive notebook cells(be it markdown or code) belonging to the same ROLE - human or llm.

The result can be produced in several formats: JSON and Markdown and csv files for manual view via `reports` and `run`, list of dicts via `review_notebooks` or a `pandas.DataFrame` with aggregated scores and feedback texts for each turn as single values.

Parallelization is achieved using concurrent module with ThreadPoolExecutor.
Parallelization is happening in 2 ways - each review category for each turn is turned into a separate task and we also process complete notebooks in parallel.

The `config.json` contains the following parameters:

- API request parameters for each reviewer
- Notebook role headers syntax for parsing purposes
- Conversation start header

API key is to be placed into `secrets.json` outside the package codebase, see `secrets.example.json` for the format example.

The following resources are used to control the review process, see `llm_reviewer/prompts` directory.:

- **System Prompts**: Located in `llm_reviewer/prompts`, these files contain the instructions given to the GPT API for generating reviews.
- **JSON Schema**: Defines the output format for reviews with specific pointers on the format inside schema description.
- **Evaluation Rubrics**: Provides the scoring criteria for reviews, detailing what constitutes different levels of code and language quality.

# Useful fucntions and ways to run

To review multiple Jupyter notebooks and convert the reviews into a pandas DataFrame, you can use the following functions:

```python
from src.llm_reviewer.notebook_reviewer import review_notebooks, notebooks_reviews_to_df
from src.llm_reviewer.run import load_notebooks_from_folder

# Assuming you have a list of notebook paths
notebook_paths = ["path/to/notebook1.ipynb", "path/to/notebook2.ipynb"]

# OR

# Load all notebook paths from a specified folder
folder_path = "path/to/notebooks_folder"
notebook_paths = load_notebooks_from_folder(folder_path)


# Review the notebooks with specified concurrency parameters
reviews = review_notebooks(notebook_paths, max_threads_per_notebook=3, max_concurrent_notebooks=2)

# Convert the reviews to a pandas DataFrame
df = notebook_reviews_to_df(reviews)
```

This will produce a list of review results for each notebook, which can then be converted into a DataFrame for further analysis or reporting. Below is an example of what the reviews output and DataFrame might look like:

Example `reviews` output:

The `reviews` output is a list of dictionaries, where each dictionary represents a single notebook's review. Each dictionary contains the following keys:

- `turns`: A list of dictionaries, each representing a turn in the conversation. Each turn has a `turn` key with a list of role dictionaries (`human` and `llm`), and `english_review` and `code_review` keys with feedback and scores.

- `nb_path`: A string indicating the path to the reviewed notebook.

```python
{
    'turns': [
        {
            'turn': [
                {
                    'role': 'human',
                    'steps': [
                        {
                            'type': 'markdown',
                            'content': "..."
                        }
                    ]
                },
                {
                    'role': 'llm',
                    'steps': [
                        {
                            'type': 'markdown',
                            'content': "..."
                        },
                        {
                            'type': 'code',
                            'content': '...'
                        }
                    ]
                }
            ],
            'english_review': {
                'feedback_text': "...",
                'score': 4
            },
            'code_review': {
                'feedback_text': "...",
                'score': 4
            }
        }
    ],
    'nb_path': 'tests/samples/1T__viz__casual.ipynb'
}
```

Example DataFrame `df`:

```python
   nb_path                                code_score  lang_score  feedback
0  tests/samples/2T__logging.ipynb        3.50        3.000000    "#Turn 1:\n\n## Language(4/5):\n- The phrase 's..."
1  tests/samples/3T__parallel.ipynb       3.00        3.666667    "#Turn 1:\n\n## Language(3/5):\n- The phrase 's..."
2  tests/samples/4T__leet_medium.ipynb    2.75        3.750000    "#Turn 1:\n\n## Language(3/5):\n- The explanati..."
3  tests/samples/1T__viz__casual.ipynb    4.00        4.000000    "#Turn 1:\n\n## Language(4/5):\n- The phrase 'O..."
```

The DataFrame provides a tabular view of the reviews, making it easy to sort, filter, and analyze the results.

Example with item 3 feedback:

```
#Turn 1:

## Language(4/5):
- The phrase 'Oh man' is informal and may not align with the professional tone expected in technical assistance.
- The phrase 'kinda easy to use' could be replaced with 'relatively easy to use' for a more formal tone.
- The phrase 'can spit out' is colloquial and could be replaced with 'can generate' or 'can produce' for clarity and professionalism.
- The function documentation could be more descriptive, explaining what 'sequence' is expected to be and what the function returns.

## Code(4/5):
- The function `plot_counts` uses `nbins=sequence.nunique()` which may not always be the optimal number of bins for a histogram. It's better to let `plotly` decide the default or provide a way for the user to specify the number of bins.
- The function `plot_counts` is not general enough to handle cases where the input is not a pandas Series. It would be more versatile if it could handle lists or numpy arrays as well.
- The function lacks error handling which could be useful to provide more informative messages if incorrect data types are passed.
- The example dataframe `df` is hardcoded, which is fine for a demonstration but should be noted that in practice, data would come from a dynamic source.
```
