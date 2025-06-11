#!/usr/bin/env python3

import sys
import os
import argparse
from pathlib import Path

# Add the parent directory to sys.path to allow importing the module
sys.path.append(str(Path(__file__).parent.parent))
from src.clickhouse_mcp.docs_search import (
    load_chunks, 
    simple_search, 
    sample_random_chunks,
    format_chunk_preview
)
from src.clickhouse_mcp.vector_search import (
    load_faiss_index, 
    vector_search,
    get_default_index_path
)
from src.clickhouse_mcp import DEFAULT_BEDROCK_MODEL, DEFAULT_REGION


def display_search_results(results, query, limit):
    """Display search results with consistent formatting.
    
    Args:
        results: List of search results
        query: Search query
        limit: Text limit for content preview
    """
    print(f"\nTop {len(results)} results for '{query}':\n")
    
    for i, result in enumerate(results):
        print(f"--- Result {i+1} ---")
        
        # Handle different result formats (dict vs Document)
        if hasattr(result, 'page_content') and hasattr(result, 'metadata'):
            # Document object from vector search
            content = result.page_content
            metadata = result.metadata
        else:
            # Dict from simple search
            content = result['content']
            metadata = result['metadata']
            
        print(f"Document: {metadata['document_title']}")
        print(f"Section: {metadata['section_title']}")
        print(f"Source: {metadata['path']}")
        
        print(f"Content: {content[:limit]}")
        if len(content) > limit:
            print("...")
        print("\n")


def main():
    parser = argparse.ArgumentParser(description='Query ClickHouse documentation chunks')
    parser.add_argument('--pickle', type=str, 
                        help='Path to the pickle file containing document chunks')
    parser.add_argument('--query', type=str,
                        help='Search query')
    parser.add_argument('-n', '--num-results', type=int, default=3,
                        help='Number of search results to return (default: 3)')
    parser.add_argument('--limit', type=int, default=100,
                        help='Text context size limit for each result (characters) (default: 100)')
    parser.add_argument('--sample', type=int,
                        help='Randomly sample N chunks from the collection')
    parser.add_argument('--semantic', action='store_true',
                        help='Use semantic search with FAISS index instead of keyword search')
    parser.add_argument('--index-path', type=str, 
                        help=f'Path to the FAISS index (default: {get_default_index_path()})')
    parser.add_argument('--model', type=str, default=DEFAULT_BEDROCK_MODEL,
                        help=f'Bedrock model ID for embeddings (default: {DEFAULT_BEDROCK_MODEL})')
    parser.add_argument('--region', type=str, default=DEFAULT_REGION,
                        help=f'AWS region name (default: {DEFAULT_REGION})')
    
    args = parser.parse_args()
    
    if not args.query and not args.sample:
        parser.error("Either --query or --sample must be provided")
    
    if args.sample:
        # Load chunks and sample
        chunks = load_chunks(args.pickle)
        print(f"Loaded {len(chunks)} document chunks")
        
        # Sample random chunks
        random_chunks = sample_random_chunks(chunks, args.sample)
        print(f"\nRandom sample of {len(random_chunks)} chunks:\n")
        for i, chunk in enumerate(random_chunks):
            print(format_chunk_preview(chunk, i+1, context_limit=args.limit))
            print("\n")
        return
    
    if args.semantic:
        # Use semantic search with FAISS index
        index_path = args.index_path or get_default_index_path()
        
        if not os.path.exists(index_path):
            print(f"Error: FAISS index not found at {index_path}")
            print("Run create_faiss_index.py first to create the index")
            return
        
        try:
            from langchain_aws import BedrockEmbeddings
        except ImportError:
            print("Required packages not found. Install with:")
            print("pip install langchain-aws faiss-cpu langchain langchain-community")
            return
        
        try:
            # Initialize embeddings
            embeddings = BedrockEmbeddings(
                region_name=args.region,
                model_id=args.model
            )
            
            # Load the index
            print(f"Loading FAISS index from {index_path}...")
            vector_store = load_faiss_index(index_path, embeddings)
            
            # Perform semantic search
            print(f"Performing semantic search for '{args.query}'...")
            results = vector_search(vector_store, args.query, args.num_results)
            
            # Display results
            display_search_results(results, args.query, args.limit)
            
        except Exception as e:
            print(f"Error during semantic search: {e}")
            print("Make sure AWS credentials are properly set with Bedrock access.")
            print("Required environment variables: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
    else:
        # Use regular keyword search
        chunks = load_chunks(args.pickle)
        print(f"Loaded {len(chunks)} document chunks")
        
        # Perform simple search
        results = simple_search(chunks, args.query, args.num_results)
        
        # Display results
        display_search_results(results, args.query, args.limit)
        
        # Note about semantic search
        print("\nTip: For more accurate semantic search, use the --semantic flag:")
        print("  python query_docs.py --query 'your query' --semantic")


if __name__ == "__main__":
    main()