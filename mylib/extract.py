import requests
from dotenv import load_dotenv
import os
import base64

# Load environment variables
load_dotenv()
server_host = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
filestore_path = "dbfs:/FileStore/tr2bx_mini_project11"
headers = {'Authorization': f'Bearer {access_token}'}
api_url = f"https://{server_host}/api/2.0"


def perform_query(path, headers, data=None):
    """Helper function to send POST requests to the Databricks API."""
    if data is None:
        data = {}
    response = requests.post(api_url + path, headers=headers, json=data)
    response.raise_for_status()
    return response.json()


def mkdirs(directory_path, headers):
    """Create directories in Databricks FileStore."""
    data = {"path": directory_path}
    response = requests.post(f"{api_url}/dbfs/mkdirs", headers=headers, json=data)
    response.raise_for_status()
    return response.json()


def upload_file(url, dbfs_path, headers):
    """Download a file from a URL and upload it to Databricks FileStore."""
    response = requests.get(url)
    response.raise_for_status()

    content = base64.b64encode(response.content).decode()
    create_response = perform_query(
        "/dbfs/create", headers, data={"path": dbfs_path, "overwrite": True}
    )
    handle = create_response["handle"]

    chunk_size = 2**20  # 1 MB
    for i in range(0, len(content), chunk_size):
        chunk = content[i : i + chunk_size]
        perform_query(
            "/dbfs/add-block", headers, data={"handle": handle, "data": chunk}
        )

    perform_query("/dbfs/close", headers, data={"handle": handle})


def extract(
    urls=None,
    dbfs_paths=None,
    directory=filestore_path,
):
    """Extract data files from URLs and upload to Databricks FileStore."""
    if urls is None:
        urls = ["https://github.com/fivethirtyeight/data/raw/master/airline-safety/airline-safety.csv"]
    if dbfs_paths is None:
        dbfs_paths = [f"{filestore_path}/airline_safety.csv"]

    mkdirs(directory, headers)
    for url, dbfs_path in zip(urls, dbfs_paths):
        upload_file(url, dbfs_path, headers)
