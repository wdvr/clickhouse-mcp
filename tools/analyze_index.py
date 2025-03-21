#!/usr/bin/env python3

import sys
from typing import Dict, Any, List
from clickhouse_mcp.docs_search import load_chunks, get_default_pickle_path

def analyze_chunks(chunks: List[Dict[str, Any]], num_examples: int = 5):
    """
    Analyze document chunks to find largest and smallest chunks.
    
    Args:
        chunks: List of document chunks to analyze
        num_examples: Number of examples to show for each category
    """
    # Add size information to each chunk
    sized_chunks = []
    for chunk in chunks:
        content = chunk['content']
        size = len(content)
        sized_chunks.append((size, chunk))
    
    # Sort by size (ascending)
    sized_chunks.sort(key=lambda x: x[0])
    
    # Get smallest and largest chunks
    smallest_chunks = sized_chunks[:num_examples]
    largest_chunks = sized_chunks[-num_examples:]
    
    # Calculate average chunk size
    total_size = sum(size for size, _ in sized_chunks)
    avg_size = total_size / len(sized_chunks)
    
    # Print results
    print(f"Total chunks: {len(chunks)}")
    print(f"Average chunk size: {avg_size:.2f} characters\n")
    
    print(f"Smallest {num_examples} chunks:")
    for i, (size, chunk) in enumerate(smallest_chunks):
        key = chunk['metadata'].get('chunk_key', 'N/A')
        path = chunk['metadata'].get('path', 'N/A')
        title = chunk['metadata'].get('section_title', 'N/A')
        print(f"{i+1}. Size: {size} chars, Key: {key}")
        print(f"   Path: {path}")
        print(f"   Title: {title}")
        print(f"   Content preview: {chunk['content'][:100]}...")
        print()
    
    print(f"\nLargest {num_examples} chunks:")
    for i, (size, chunk) in enumerate(reversed(largest_chunks)):
        key = chunk['metadata'].get('chunk_key', 'N/A')
        path = chunk['metadata'].get('path', 'N/A')
        title = chunk['metadata'].get('section_title', 'N/A')
        print(f"{i+1}. Size: {size} chars, Key: {key}")
        print(f"   Path: {path}")
        print(f"   Title: {title}")
        print(f"   Content preview: {chunk['content'][:100]}...")
        print()

def main():
    # Load chunks from default location
    try:
        chunks = load_chunks()
        print(f"Loaded {len(chunks)} chunks from {get_default_pickle_path()}")
        
        # Analyze chunks
        analyze_chunks(chunks)
        
    except Exception as e:
        print(f"Error analyzing chunks: {e}", file=sys.stderr)
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())