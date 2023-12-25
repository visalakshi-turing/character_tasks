from google.oauth2 import service_account
from googleapiclient.discovery import build


def build_service(service_account_json_secrets_path):
    scope = ["https://www.googleapis.com/auth/drive"]
    service_account_json_key = service_account_json_secrets_path
    credentials = service_account.Credentials.from_service_account_file(
        filename=service_account_json_key, scopes=scope
    )
    service = build("drive", "v3", credentials=credentials)
    return service
