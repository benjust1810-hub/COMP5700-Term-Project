#!/usr/bin/env python3

"""
generate_aidev_csvs.py

Downloads required AIDev parquet files from the HuggingFace dataset and produces:
 - task1_all_pull_request.csv
 - task2_all_repository.csv
 - task3_pr_task_type.csv
 - task4_pr_commit_details.csv
 - task5_combined_security.csv

Usage:
  pip install -U pandas pyarrow huggingface_hub
  python generate_aidev_csvs.py
"""

import re
import sys
import os
from pathlib import Path

import pandas as pd
from huggingface_hub import hf_hub_download

OUT_DIR = Path("aidev_csv_outputs")
OUT_DIR.mkdir(exist_ok=True)

# Files that will download from the HuggingFace dataset (repo_id "hao-li/AIDev").
REPO_ID = "hao-li/AIDev"
FILES = {
    "all_pull_request": "all_pull_request.parquet",
    "all_repository": "all_repository.parquet",
    "pr_task_type": "pr_task_type.parquet",
    "pr_commit_details": "pr_commit_details.parquet",
}

def download_parquet(name, filename):
    print(f"Downloading {filename} from HuggingFace dataset hao-li/AIDev ...")
    local_path = hf_hub_download(
        repo_id="hao-li/AIDev",
        filename=filename,
        repo_type="dataset"  # IMPORTANT
    )
    print(f" -> saved to: {local_path}")
    return local_path
def clean_diff(text):
    """
    This removes special or non printable control characters from the diffs in order to avoid encoding issues.
    This also replaces newlines w/ single spaces to keep the diff in a singular CSV cell.
    """
    if pd.isna(text):
        return ""
    # Convert to str.
    s = str(text)
    # Replace newline/tab etc. with single space.
    s = re.sub(r'[\r\n\t]+', ' ', s)
    # Remove other non-printable characters.
    s = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', s)
    # Collapses repeated spaces if applicable.
    s = re.sub(r' {2,}', ' ', s).strip()
    return s

def produce_task1(all_pull_request_path):
    print("Producing Task-1 CSV (all_pull_request fields)...")
    df = pd.read_parquet(all_pull_request_path, engine="pyarrow")
    out = df.rename(columns={
        "title": "TITLE",
        "id": "ID",
        "agent": "AGENTNAME",
        "body": "BODYSTRING",
        "repo_id": "REPOID",
        "repo_url": "REPOURL"
    })
    out = out[["TITLE", "ID", "AGENTNAME", "BODYSTRING", "REPOID", "REPOURL"]]
    path = OUT_DIR / "task1_all_pull_request.csv"
    out.to_csv(path, index=False, encoding="utf-8")
    print(f" -> Wrote {path} ({len(out):,} rows)")

def produce_task2(all_repository_path):
    print("Producing Task-2 CSV (all_repository fields)...")
    df = pd.read_parquet(all_repository_path, engine="pyarrow")
    out = df.rename(columns={
        "id": "REPOID",
        "language": "LANG",
        "stars": "STARS",
        "url": "REPOURL"
    })
    out = out[["REPOID", "LANG", "STARS", "REPOURL"]]
    path = OUT_DIR / "task2_all_repository.csv"
    out.to_csv(path, index=False, encoding="utf-8")
    print(f" -> Wrote {path} ({len(out):,} rows)")

def produce_task3(pr_task_type_path):
    print("Producing Task-3 CSV (pr_task_type fields)...")
    df = pd.read_parquet(pr_task_type_path, engine="pyarrow")
    out = df.rename(columns={
        "id": "PRID",
        "title": "PRTITLE",
        "reason": "PRREASON",
        "type": "PRTYPE",
        "confidence": "CONFIDENCE",
    })
    out = out[["PRID", "PRTITLE", "PRREASON", "PRTYPE", "CONFIDENCE"]]
    path = OUT_DIR / "task3_pr_task_type.csv"
    out.to_csv(path, index=False, encoding="utf-8")
    print(f" -> Wrote {path} ({len(out):,} rows)")

def produce_task4(pr_commit_details_path):
    print("Producing Task-4 CSV (pr_commit_details fields)...")
    df = pd.read_parquet(pr_commit_details_path, engine="pyarrow")
    out = df.rename(columns={
        "pr_id": "PRID",
        "sha": "PRSHA",
        "message": "PRCOMMITMESSAGE",
        "filename": "PRFILE",
        "status": "PRSTATUS",
        "additions": "PRADDS",
        "deletions": "PRDELSS",
        "changes": "PRCHANGECOUNT",
        "patch": "PRDIFF"
    })
    # Cleans the PRDIFF to remove special characters
    out["PRDIFF"] = out["PRDIFF"].apply(clean_diff)
    out = out[["PRID", "PRSHA", "PRCOMMITMESSAGE", "PRFILE", "PRSTATUS",
               "PRADDS", "PRDELSS", "PRCHANGECOUNT", "PRDIFF"]]
    path = OUT_DIR / "task4_pr_commit_details.csv"
    out.to_csv(path, index=False, encoding="utf-8")
    print(f" -> Wrote {path} ({len(out):,} rows)")

def produce_task5(task1_csv_path, task3_csv_path, task4_csv_path=None):
    """
    Produces a combined CSV:
    ID: ID of the pull request
    AGENT: agent name (from task1)
    TYPE: type of the pull request (from task3 and PRTYPE)
    CONFIDENCE: confidence (from task3)
    SECURITY: 1 if any security-related keyword appears in body or title, else 0
    """

    # Security keywords as provided in assignment instructions.
    security_keywords = [
        "race","racy","buffer","overflow","stack","integer","signedness","underflow",
        "improper","unauthenticated","gain access","permission","cross site","css",
        "xss","denial service","dos","crash","deadlock","injection",
        "request forgery","csrf","xsrf","forged","security","vulnerability",
        "vulnerable","exploit","attack","bypass","backdoor","threat","expose",
        "breach","violate","fatal","blacklist","overrun","insecure"
    ]
    keywords = [k.lower() for k in security_keywords]

    df1 = pd.read_csv(task1_csv_path, encoding="utf-8")
    df3 = pd.read_csv(task3_csv_path, encoding="utf-8")

    merged = df1.merge(df3, left_on="ID", right_on="PRID", how="left", suffixes=("", "_task3"))

    def has_security_flag(row):
        title = "" if pd.isna(row.get("TITLE")) else str(row.get("TITLE")).lower()
        body = "" if pd.isna(row.get("BODYSTRING")) else str(row.get("BODYSTRING")).lower()
        text = f"{title} {body}"
        # Check for any keyword presence. For multi-word keywords, simple substring match is fine.
        for kw in keywords:
            if kw in text:
                return 1
        return 0

    print("Scanning for security keywords to create SECURITY flag (Task-5)...")
    merged["SECURITY"] = merged.apply(has_security_flag, axis=1)
    out = pd.DataFrame({
        "ID": merged["ID"],
        "AGENT": merged["AGENTNAME"],
        "TYPE": merged["PRTYPE"],
        "CONFIDENCE": merged["CONFIDENCE"],
        "SECURITY": merged["SECURITY"]
    })
    path = OUT_DIR / "task5_combined_security.csv"
    out.to_csv(path, index=False, encoding="utf-8")
    print(f" -> Wrote {path} ({len(out):,} rows)")

def main():
    local_paths = {}
    for key, fname in FILES.items():
        try:
            local_paths[key] = download_parquet(key, fname)
        except Exception as e:
            print(f"ERROR downloading {fname}: {e}", file=sys.stderr)
            print("If the file is large you may need to authenticate with HF or download manually from\n"
                  f"https://huggingface.co/datasets/{REPO_ID}", file=sys.stderr)
            raise

    produce_task1(local_paths["all_pull_request"])
    produce_task2(local_paths["all_repository"])
    produce_task3(local_paths["pr_task_type"])
    produce_task4(local_paths["pr_commit_details"])

    produce_task5(OUT_DIR / "task1_all_pull_request.csv", OUT_DIR / "task3_pr_task_type.csv")

    print("All done. CSVs are in:", OUT_DIR.resolve())

if __name__ == "__main__":
    main()