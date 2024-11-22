"""
Test Databricks Functionality
"""
import requests
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
server_host = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/tr2bx_mini_project11"
API_URL = f"https://{server_host}/api/2.0"


def check_dbfs_path_exists(path, headers):
    """
    Check if a given DBFS path exists using the Databricks API.

    Args:
        path (str): The DBFS path to check.
        headers (dict): HTTP headers including the authorization token.

    Returns:
        bool: True if the path exists, False otherwise.
    """
    try:
        response = requests.get(
            f"{API_URL}/dbfs/get-status",
            headers=headers,
            params={"path": path},
        )
        response.raise_for_status()
        return True  # Path exists
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"Path not found: {path}")
        else:
            print(f"Error checking DBFS path: {e}")
        return False


def test_databricks():
    """
    Test Databricks functionality by verifying the existence of a directory
    and a specific file in DBFS.
    """
    headers = {"Authorization": f"Bearer {access_token}"}

    # Check if the directory exists
    assert check_dbfs_path_exists(
        FILESTORE_PATH, headers
    ), "Directory does not exist in DBFS"

    # Check if the file exists in the directory
    file_path = f"{FILESTORE_PATH}/airline_safety.csv"
    assert check_dbfs_path_exists(
        file_path, headers
    ), f"File {file_path} does not exist in DBFS"


if __name__ == "__main__":
    test_databricks()
