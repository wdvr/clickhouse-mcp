#!/usr/bin/env python

import unittest
import sys
import os
import subprocess
from pathlib import Path

def ensure_clickhouse_docs():
    """Ensure ClickHouse docs are available before running tests."""
    docs_path = Path(__file__).resolve().parent / "clickhouse_docs"
    
    if not docs_path.exists():
        print("ClickHouse docs not found. Running checkout script...")
        checkout_script = Path(__file__).resolve().parent / "tools" / "checkout_clickhouse_docs.py"
        
        if not checkout_script.exists():
            print(f"Error: Checkout script not found at {checkout_script}")
            return False
            
        result = subprocess.run([sys.executable, str(checkout_script)], check=False)
        
        if result.returncode != 0:
            print("Failed to checkout ClickHouse docs. Tests requiring docs may fail.")
            return False
    
    return True

def is_excluded_dir(dir_path):
    excluded = ['venv']  # Removed clickhouse since we're using clickhouse_docs now
    for excluded_dir in excluded:
        if excluded_dir in dir_path.split(os.sep):
            return True
    return False

if __name__ == "__main__":
    # Ensure ClickHouse docs are available before running tests
    if not ensure_clickhouse_docs():
        print("Warning: Could not ensure ClickHouse docs are available.")

    test_loader = unittest.TestLoader()
    
    # Define a pattern matching function to exclude certain directories
    original_discover = test_loader.discover
    
    def patched_discover(start_dir, pattern='test*.py', top_level_dir=None):
        if is_excluded_dir(start_dir):
            # Return empty test suite for excluded directories
            return unittest.TestSuite()
        return original_discover(start_dir, pattern, top_level_dir)
    
    # Apply the patch
    test_loader.discover = patched_discover
    
    # Discover tests
    test_suite = test_loader.discover('.')
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    sys.exit(not result.wasSuccessful())
