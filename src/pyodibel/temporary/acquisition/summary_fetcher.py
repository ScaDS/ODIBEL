import hashlib
import requests
import os
from tqdm import tqdm
from time import sleep

def hash_uri(uri: str) -> str:
    """Hash a URI to create a filename-safe string."""
    return hashlib.md5(uri.encode('utf-8')).hexdigest()

def read_uris_from_file(file_path: str) -> list[str]:
    """Read URIs from a file."""
    with open(file_path, 'r') as file:
        return [line.strip() for line in file.readlines()]


def fetch_summary(uri: str) -> str:
    """Fetch a summary for a given URI."""
    # 50 ms delay
    sleep(0.05)
    prefix = uri.split("/")[-1]
    url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{prefix}"
    try:
        response = requests.get(url)
        if response.status_code == 429:
            # Too Many Requests, wait 5 seconds and try again
            sleep(5)
            response = requests.get(url)
        response.raise_for_status()
        return response.json()["extract"]
    except requests.exceptions.RequestException as e:
        print(f"Error fetching summary for {uri}: {e}")
        return ""

def fetch_and_save_summary(uri: str, output_dir: str) -> bool:
    """Fetch a summary for a URI and save it immediately. Returns True if saved, False if skipped."""
    summary_file = os.path.join(output_dir, f"{hash_uri(uri)}.txt")
    
    # Skip if summary already exists
    if os.path.exists(summary_file):
        return False
    
    summary = fetch_summary(uri)
    if summary:
        os.makedirs(output_dir, exist_ok=True)
        with open(summary_file, 'w') as file:
            file.write(summary)
        return True
    return False

def fetch_summaries(uris: list[str], output_dir: str) -> dict[str, str]:
    """Fetch summaries for a list of URIs and save them immediately."""
    summaries = {}
    for uri in tqdm(uris):
        summary_file = os.path.join(output_dir, f"{hash_uri(uri)}.txt")
        
        # Try to fetch and save the summary
        was_saved = fetch_and_save_summary(uri, output_dir)
        
        # Read the summary file (whether it was just saved or already existed)
        try:
            with open(summary_file, 'r') as file:
                summaries[uri] = file.read()
        except FileNotFoundError:
            # If file doesn't exist, it means the summary fetch failed and no file was created
            summaries[uri] = ""
    
    return summaries


def save_summaries(summaries: dict[str, str], output_dir: str) -> None:
    """Save summaries to a file."""
    os.makedirs(output_dir, exist_ok=True)
    for uri, summary in summaries.items():
        with open(os.path.join(output_dir, f"{hash_uri(uri)}.txt"), 'w') as file:
            file.write(summary)

if __name__ == "__main__":
    uris = read_uris_from_file("/home/marvin/project/data/acquisiton/selection_dbp_a_film_1k_deg_gt_9.txt")
    summaries = fetch_summaries(uris, "summaries")