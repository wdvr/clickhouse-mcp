#!/usr/bin/env python3

import sys
import argparse
from pathlib import Path

# Add the parent directory to sys.path to allow importing the module
sys.path.append(str(Path(__file__).parent.parent))
from src.clickhouse_mcp.docs_search import (
    load_chunks, 
    simple_search, 
    get_context_snippet,
    sample_random_chunks,
    format_chunk_preview
)


def main():
    parser = argparse.ArgumentParser(description='Query ClickHouse documentation chunks')
    parser.add_argument('--pickle', type=str, 
                        help='Path to the pickle file containing document chunks')
    parser.add_argument('--query', type=str,
                        help='Search query')
    parser.add_argument('-n', '--num-results', type=int, default=1,
                        help='Number of search results to return (default: 1)')
    parser.add_argument('--limit', type=int, default=50,
                        help='Text context size limit for each result (characters before and after match)')
    parser.add_argument('--sample', type=int,
                        help='Randomly sample N chunks from the collection')
    
    args = parser.parse_args()
    
    if not args.query and not args.sample:
        parser.error("Either --query or --sample must be provided")
    
    # Load the chunks
    chunks = load_chunks(args.pickle)
    print(f"Loaded {len(chunks)} document chunks")
    
    if args.sample:
        # Sample random chunks
        random_chunks = sample_random_chunks(chunks, args.sample)
        print(f"\nRandom sample of {len(random_chunks)} chunks:\n")
        for i, chunk in enumerate(random_chunks):
            print(format_chunk_preview(chunk, i+1, context_limit=args.limit))
            print("\n")
    
    elif args.query:
        # Perform simple search
        results = simple_search(chunks, args.query, args.num_results)
        
        # Display results
        print(f"\nTop {len(results)} results for '{args.query}':\n")
        for i, result in enumerate(results):
            print(f"--- Result {i+1} ---")
            print(f"Document: {result['metadata']['document_title']}")
            print(f"Section: {result['metadata']['section_title']}")
            print(f"Source: {result['metadata']['path']}")
            
            # Print a snippet of the content around the query words
            context = get_context_snippet(result['content'], args.query, args.limit)
            print(f"Context: {context}")
            print("\n")
        
        # Note about using with vector embeddings
        print("Note: For more accurate semantic search, use a vector database like FAISS:")
        print("  from langchain.vectorstores import FAISS")
        print("  from langchain.embeddings import OpenAIEmbeddings")
        print("  embeddings = OpenAIEmbeddings()")
        print("  vector_index = FAISS.from_documents(chunks, embeddings)")
        print("  results = vector_index.similarity_search(query)")


if __name__ == "__main__":
    main()