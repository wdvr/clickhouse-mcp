#!/usr/bin/env python

import unittest
import sys
import os

def is_excluded_dir(dir_path):
    excluded = ['clickhouse', 'venv']
    for excluded_dir in excluded:
        if excluded_dir in dir_path.split(os.sep):
            return True
    return False

if __name__ == "__main__":
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