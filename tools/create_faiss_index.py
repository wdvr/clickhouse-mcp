#!/usr/bin/env python3

"""
Create a FAISS index from document chunks.

This tool creates a FAISS vector index using AWS Bedrock embeddings from the document
chunks stored in the pickle file. The tool supports full indexing mode and a test mode
with limited documents. It can also filter documents by a query before embedding.

The tool also offers a print-only mode that displays chunk information without
creating embeddings or an index, which is useful for exploring and analyzing chunks.

Requirements:
- AWS credentials with Bedrock access (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)
  (only required when not using --print-only mode)
- Required packages: langchain-aws, faiss-cpu, langchain

Usage:
    # Create full index
    python create_faiss_index.py

    # Test mode with limited documents (using num_results to set limit)
    python create_faiss_index.py --test -n 10 --output ./index/test_faiss_index

    # Test mode with query filtering (using num_results to control returned chunks)
    python create_faiss_index.py --test --query "table creation" -n 15 --output ./index/test_faiss_index
    
    # Print-only mode to examine chunks without creating embeddings
    python create_faiss_index.py --print-only --query "table creation" -n 5
    
    # Print-only mode with custom preview length
    python create_faiss_index.py --print-only --preview-length 500
"""

import sys
import argparse
from pathlib import Path

# Add the parent directory to sys.path to allow importing the module
sys.path.append(str(Path(__file__).parent.parent))
from src.clickhouse_mcp.docs_search import (
    load_chunks, 
    simple_search,
    get_default_pickle_path
)
from src.clickhouse_mcp.vector_search import create_faiss_index, get_default_index_path
from src.clickhouse_mcp import DEFAULT_BEDROCK_MODEL, DEFAULT_REGION


def print_chunk_preview(chunk, index, preview_length=200):
    """Print preview of a single chunk.
    
    Args:
        chunk: Document chunk to preview.
        index: Index of the chunk for display.
        preview_length: Number of characters to show in the preview.
    """
    content = chunk['content']
    content_length = len(content)
    preview = content[:preview_length] + "..." if content_length > preview_length else content
    
    print(f"Chunk {index+1}:")
    print(f"Key: {chunk['metadata'].get('chunk_key', 'Unknown')}")
    print(f"Document: {chunk['metadata'].get('document_title', 'Unknown')}")
    print(f"Section: {chunk['metadata'].get('section_title', 'Unknown')}")
    print(f"Path: {chunk['metadata'].get('path', 'Unknown')}")
    print(f"Content length: {content_length} characters")
    print(f"Preview: {preview}")
    print("-" * 80)


def main():
    parser = argparse.ArgumentParser(description='Create FAISS index from document chunks')
    default_pickle = str(get_default_pickle_path())
    default_index_path = str(get_default_index_path())
    
    parser.add_argument('--pickle', type=str, 
                        help=f'Path to the pickle file containing document chunks (default: {default_pickle})')
    parser.add_argument('--output', type=str, default=default_index_path,
                        help=f'Path where to save the FAISS index (default: {default_index_path})')
    parser.add_argument('--test', action='store_true',
                        help='Run in test mode with limited number of documents')
    parser.add_argument('--query', type=str,
                        help='Filter chunks by query before embedding (for test mode)')
    parser.add_argument('-n', '--num-results', type=int, default=10,
                        help='Number of chunks to process in test mode, or number of results to return when using --query (default: 10)')
    parser.add_argument('--model', type=str, default=DEFAULT_BEDROCK_MODEL,
                        help=f'Bedrock model ID for embeddings (default: {DEFAULT_BEDROCK_MODEL})')
    parser.add_argument('--region', type=str, default=DEFAULT_REGION,
                        help=f'AWS region name (default: {DEFAULT_REGION})')
    parser.add_argument('--print-only', action='store_true',
                        help='Only print chunk information without creating embeddings or index')
    parser.add_argument('--preview-length', type=int, default=200,
                        help='Number of characters to show in content preview (default: 200)')
    
    args = parser.parse_args()
    
    # Load the chunks
    pickle_path = args.pickle if args.pickle else str(get_default_pickle_path())
    chunks = load_chunks(pickle_path)
    print(f"Loaded {len(chunks)} document chunks from {pickle_path}")
    
    # If test mode is enabled, set limit to num_results
    limit = args.num_results if args.test and not args.query else None
    if args.test and not args.query:
        print(f"Test mode: limiting to {limit} chunks")
    
    # Filter chunks if query is provided
    if args.query:
        print(f"Filtering chunks with query: '{args.query}'")
        filtered_chunks = simple_search(chunks, args.query, args.num_results)
        print(f"Found {len(filtered_chunks)} chunks matching the query")
        docs_to_process = filtered_chunks
    else:
        docs_to_process = chunks
    
    # Apply limit if specified
    if limit and limit < len(docs_to_process):
        print(f"Limiting to {limit} chunks")
        docs_to_process = docs_to_process[:limit]
    
    # If print-only mode is enabled, just display chunk information
    if args.print_only:
        print(f"\nDisplaying {len(docs_to_process)} chunks:")
        print("-" * 80)
        
        for i, chunk in enumerate(docs_to_process):
            print_chunk_preview(chunk, i, args.preview_length)
        
        return
    
    # Import required packages for vector embeddings
    try:
        from langchain_aws import BedrockEmbeddings
    except ImportError:
        print("Required packages not found. Install with:")
        print("pip install langchain-aws faiss-cpu langchain langchain-community")
        raise
    
    # Initialize Bedrock Embeddings
    try:
        embeddings = BedrockEmbeddings(
            region_name=args.region,
            model_id=args.model
        )
    except Exception as e:
        print(f"Error initializing Bedrock embeddings: {e}")
        print("Make sure AWS credentials are properly set with Bedrock access.")
        print("Required environment variables: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        raise
    
    print(f"Using embedding model: {args.model}")
    
    # Create the FAISS index with the processed chunks
    create_faiss_index(
        chunks=docs_to_process,
        output_path=args.output,
        embeddings=embeddings
    )
    
    # Example usage of the created index
    print("\nExample usage of the created FAISS index:")
    print("```python")
    print("from langchain_aws import BedrockEmbeddings")
    print("from src.clickhouse_mcp.vector_search import load_faiss_index, vector_search")
    print("")
    print("# Load the index")
    print(f"embeddings = BedrockEmbeddings(region_name='{args.region}', model_id='{args.model}')")
    print(f"vector_store = load_faiss_index('{args.output}', embeddings)")
    print("")
    print("# Search the index")
    print("results = vector_search(vector_store, 'how to create a table in ClickHouse', 3)")
    print("for doc in results:")
    print("    print(doc.metadata['document_title'])")
    print("    print(doc.metadata['section_title'])")
    print("    print(doc.page_content[:100] + '...')")
    print("    print()")
    print("```")


if __name__ == "__main__":
    main()