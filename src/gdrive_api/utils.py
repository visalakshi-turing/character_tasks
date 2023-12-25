import re
import json
from typing import Optional, List, Dict, Set
from googleapiclient.discovery import Resource

import os
from openai import OpenAI

openai_api_key = os.environ.get("OPENAI_API_KEY")
client = OpenAI()

def extract_file_id(file: str, is_url: bool = True) -> str:
    """Extract the file ID from a Google Drive file URL or ID.

    Args:
        file: The URL or ID of the file in Google Drive.
        is_url: A flag indicating whether the provided file is a URL. Default is True.

    Returns:
        The ID of the file.
    """
    if is_url:
        try:
            file_id = file.split("/d/")[1].split("/")[0]
            if not re.match(r"^[a-zA-Z0-9_-]+$", file_id):
                raise ValueError(
                    f"Invalid file ID: {file_id}. Please provide a valid Google Drive file ID."
                )
        except IndexError:
            raise ValueError(
                f"Invalid file URL: {file}. Please provide a valid Google Drive file URL."
            )
    else:
        if "/" in file:
            raise ValueError(
                f"Invalid file ID: {file}. Please provide a valid Google Drive file ID."
            )
        file_id = file
    return file_id


def extract_folder_id(folder: str, is_url: bool = True) -> str:
    """Extract the folder ID from a Google Drive folder URL or ID.

    Args:
        folder: The URL or ID of the folder in Google Drive.
        is_url: A flag indicating whether the provided folder is a URL. Default is True.

    Returns:
        The ID of the folder.
    """
    if is_url:
        try:
            folder_id = folder.split("/folders/")[1].split("?")[0]
            if not re.match(r"^[a-zA-Z0-9_-]+$", folder_id):
                raise ValueError(
                    f"Invalid folder ID: {folder_id}. Please provide a valid Google Drive folder ID."
                )
        except IndexError:
            raise ValueError(
                f"Invalid folder URL: {folder}. Please provide a valid Google Drive folder URL."
            )
    else:
        if "/" in folder:
            raise ValueError(
                f"Invalid folder ID: {folder}. Please provide a valid Google Drive folder ID."
            )
        folder_id = folder
    return folder_id


def get_nested_folder_id(
    service: Resource, folder_path: str, parent_id: str
) -> Optional[str]:
    """Retrieve the ID of a nested folder in Google Drive using a path.

    Args:
        service: The Google Drive service resource.
        folder_path: The path of the folder to find, may include nested folders.
        parent_id: The ID of the parent folder.

    Returns:
        The ID of the nested folder or None if not found.
    """
    folder_names = folder_path.strip("/").split("/")
    for folder_name in folder_names:
        query = f"name = '{folder_name}' and '{parent_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        response = (
            service.files()
            .list(q=query, spaces="drive", fields="files(id, name)")
            .execute()
        )
        folders = response.get("files", [])
        if not folders:
            return None
        # Assuming the first match is the correct one, as folder names can be non-unique
        parent_id = folders[0].get("id")
    return parent_id


def create_folder_path(service: Resource, folder_path: str, parent_id: str) -> str:
    """Create a new folder path in Google Drive, creating subfolders as needed.

    Args:
        service: The Google Drive service resource.
        folder_path: The path of the folder to create, may include nested folders.
        parent_id: The ID of the parent folder.

    Returns:
        The ID of the last subfolder in the path.
    """
    folder_names = folder_path.split("/")
    for folder_name in folder_names:
        folder_id = get_nested_folder_id(service, folder_name, parent_id)
        if folder_id is None:
            file_metadata = {
                "name": folder_name,
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [parent_id],
            }
            folder = service.files().create(body=file_metadata, fields="id").execute()
            folder_id = folder.get("id")
        parent_id = folder_id
    return parent_id


def get_file_id(service: Resource, file_name: str, parent_id: str) -> Optional[str]:
    """Retrieve the ID of a file in Google Drive.

    Args:
        service: The Google Drive service resource.
        file_name: The name of the file to find.
        parent_id: The ID of the parent folder.

    Returns:
        The ID of the file or None if not found.
    """
    query = f"name = '{file_name}' and '{parent_id}' in parents and trashed = false"
    response = (
        service.files()
        .list(q=query, spaces="drive", fields="files(id, name)")
        .execute()
    )
    for file in response.get("files", []):
        if file.get("name") == file_name:
            return file.get("id")
    return None

def load_problems(file_path: str) -> List[dict]:
    """
    Load a list of problems from a JSON file.

    Parameters:
    file_path: str
        The path to the JSON file containing problems data.

    Returns:
    list
        A list of problems loaded from the JSON file, or an empty list if the file
        cannot be found or read as valid JSON.
    """
    existing_problems = []
    try:
        with open(file_path, 'r') as f:
            existing_problems = json.load(f)
    except FileNotFoundError:
        print(f"{file_path} not found.")
    except json.JSONDecodeError:
        print(f"{file_path} is not a valid JSON file.")
    
    return existing_problems

def extract_questions(problems: List[dict]) -> Set[str]:
    """
    Extract unique questions from a list of existing problems.

    Parameters:
    problems: list
        The list of problems, each of which is a dictionary that may contain a
        list of messages under the key "messages".

    Returns:
    set
        A set of unique questions asked by users in the problem messages.
    """
    existing_questions = set()

    for problem in problems:
        messages = problem.get("messages", [])
        for message in messages:
            if message.get("role") == "user":
                user_content = message.get("content")
                if user_content:
                    existing_questions.add(user_content)
    
    return existing_questions

def find_and_load_all_problems(parent_folder: str) -> List[dict]:
    """
    Look for problems.json files in each child folder, concatenate the content, and return a list of problems.

    Parameters:
    parent_folder: str
        The path to the parent directory.

    Returns:
    list
        A list of problems loaded from all the problems.json files within the child directories.
    """
    all_problems = []
    for root, dirs, files in os.walk(parent_folder):
        for file in files:
            if file == 'problems.json':
                json_file_path = os.path.join(root, file)
                loaded_problems = load_problems(json_file_path)
                all_problems.extend(loaded_problems)
                
    return all_problems

def generate_human_like_questions(topic, n=5, existing_questions=None):
    SYSTEM_PROMPT = f"""IDENTITY:
You are a world class Python developer. And you're concise and precise.

CONTEXT:
We are trying to generate human-like queries that a user would send to an ai assistant through a chat interface. 
The user's query tone & structure should be diversified as much as possible making sure to include some realistic examples.
We already have batch of previously generated questions and we want to avoid duplication of questions when generating new batch.

CONSTRAINTS:
1. Python Related
2. Easy (Solvable by a median developer in 15 minutes)
3. Questions should elicit a response that includes code.

INSTRUCTION:
You will be given a topic and an ask for number of questions to generate.
You will also be given previously generated questions for respective topic and ask to ignore generating question if exist.
The aim is to maximize diversity.
Act accordingly.

RESPONSE FORMAT:
A JSON-valid list of questions(strings) like {{"questions": ["question1", "question2", ...]}}
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4-1106-preview",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"Topic: {topic} \nNumber of questions: {n} \n existing topic {existing_questions}"},
            ],
            temperature=0.0,
            max_tokens=4096,
            seed = 42,
            response_format={ 
                "type": "json_object" 
            },
        )
        questions = json.loads(response.choices[0].message.content)
        return questions
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    

def generate_human_like_code_modification_requests(topic, n=5, existing_questions=None):
    SYSTEM_PROMPT = f"""IDENTITY:
You are a world class Python developer. And you're concise and precise.

CONTEXT:
We are trying to generate human-like code modification requests that a user would send to an ai assistant through a chat interface.
The user's query tone & structure should be diversified as much as possible making sure to include some realistic examples.
We already have batch of previously generated questions and we want to avoid duplication of questions when generating new batch.

CONSTRAINTS:
1. Python Related
2. Easy (Solvable by a median developer in 15 minutes)
3. Questions should include code along with a request to modify it.

INSTRUCTION:
You will be given a topic and an ask for number of questions to generate.
You will also be given previously generated questions for respective topic and ask to ignore generating question if exist.
The aim is to maximize diversity.
Act accordingly.

RESPONSE FORMAT:
A JSON-valid list of questions(strings) like {{"questions": ["question1", "question2", ...]}}
    """
    try:
        response = client.chat.completions.create(
            model="gpt-4-1106-preview",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"Topic: {topic} \nNumber of code modification requests: {n} \n existing topic {existing_questions}"},
            ],
            temperature=0.0,
            max_tokens=4096,
            seed = 42,
            response_format={ 
                "type": "json_object" 
            },
        )
        questions = json.loads(response.choices[0].message.content)
        return questions
    except Exception as e:
        print(f"An error occurred: {e}")
        return None