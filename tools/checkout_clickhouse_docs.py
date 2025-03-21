#!/usr/bin/env python3

import subprocess
import sys
from pathlib import Path

# Define constants
CLICKHOUSE_REPO = "https://github.com/ClickHouse/ClickHouse.git"
COMMIT_HASH = "edf8e4e6a4d8075e772fd4f1d1be310d5f22cf55"
DOCS_DIR = "docs/en"
TARGET_DIR = "clickhouse_docs"


def ensure_git_installed():
    try:
        subprocess.run(["git", "--version"], check=True, capture_output=True)
    except (subprocess.SubprocessError, FileNotFoundError):
        print("Error: git is not installed or not available in PATH", file=sys.stderr)
        sys.exit(1)


def checkout_docs():
    # Get project root directory
    project_root = Path(__file__).resolve().parent.parent
    target_path = project_root / TARGET_DIR

    # Create target directory if it doesn't exist
    if not target_path.exists():
        target_path.mkdir(parents=True)
    
    # If .git directory exists in target path, it's already a git repo
    if (target_path / ".git").exists():
        print(f"Repository already exists in {target_path}")
        # Fetch latest changes
        subprocess.run(["git", "fetch"], cwd=target_path, check=True)
    else:
        # Clone the repository with a sparse checkout
        print(f"Cloning ClickHouse repository (sparse checkout) to {target_path}")
        subprocess.run([
            "git", "clone", "--no-checkout", "--filter=blob:none", "--depth=1", 
            CLICKHOUSE_REPO, str(target_path)
        ], check=True)
        
        # Configure sparse checkout
        subprocess.run([
            "git", "sparse-checkout", "init", "--cone"
        ], cwd=target_path, check=True)
        
        subprocess.run([
            "git", "sparse-checkout", "set", DOCS_DIR
        ], cwd=target_path, check=True)
    
    # Checkout the specified commit
    print(f"Checking out commit {COMMIT_HASH}")
    subprocess.run([
        "git", "checkout", COMMIT_HASH
    ], cwd=target_path, check=True)
    
    docs_path = target_path / DOCS_DIR
    if docs_path.exists():
        print(f"ClickHouse documentation successfully checked out to {docs_path}")
        return True
    else:
        print(f"Error: Documentation directory not found at {docs_path}", file=sys.stderr)
        return False


def main():
    ensure_git_installed()
    success = checkout_docs()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()