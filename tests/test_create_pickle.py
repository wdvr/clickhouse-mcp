#!/usr/bin/env python3

import os
import sys
import pickle
from pathlib import Path

# Add the tools directory to the path so we can import chunk_md
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tools"))
import chunk_md

def create_test_pickle():
    """Create a pickle file for testing."""
    
    # Path to test docs
    test_docs_dir = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) / "test_docs"
    
    # Make sure the index directory exists
    index_dir = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) / "index"
    index_dir.mkdir(exist_ok=True)
    
    # Process each test file
    test_files = [
        test_docs_dir / "no_headers.md",
        test_docs_dir / "mixed_headers.md",
        test_docs_dir / "only_h3.md",
        test_docs_dir / "long_document.md",
    ]
    
    all_chunks = []
    for file_path in test_files:
        if file_path.exists():
            chunks = chunk_md.chunk_markdown_file(str(file_path))
            all_chunks.extend(chunks)
            print(f"Processed {file_path}: {len(chunks)} chunks")
    
    # Try to process syntax.md from ClickHouse docs if it exists
    docs_dir = chunk_md.get_docs_dir()
    syntax_md_path = docs_dir / "sql-reference" / "syntax.md"
    if syntax_md_path.exists():
        try:
            chunks = chunk_md.chunk_markdown_file(str(syntax_md_path))
            all_chunks.extend(chunks)
            print(f"Processed {syntax_md_path}: {len(chunks)} chunks")
        except Exception as e:
            print(f"Error processing {syntax_md_path}: {e}")
    
    # Save to pickle file
    pickle_path = chunk_md.get_default_output_path()
    with open(pickle_path, 'wb') as f:
        pickle.dump(all_chunks, f)
    
    print(f"Created pickle file at {pickle_path} with {len(all_chunks)} chunks")

if __name__ == "__main__":
    create_test_pickle()