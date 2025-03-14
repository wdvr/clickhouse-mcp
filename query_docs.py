#!/usr/bin/env python3

import os
import pickle
import argparse
from typing import List, Dict, Any

# Define a function to load the chunks
def load_chunks(pickle_path: str) -> List[Dict[str, Any]]:
    """Load document chunks from a pickle file."""
    with open(pickle_path, 'rb') as f:
        return pickle.load(f)

# Simple search function (without embeddings)
def simple_search(chunks: List[Dict[str, Any]], query: str, limit: int = 5) -> List[Dict[str, Any]]:
    """Simple keyword search in document chunks."""
    query = query.lower()
    # Score each chunk based on the query (simple word matching)
    scored_chunks = []
    for chunk in chunks:
        content = chunk['content'].lower()
        score = 0
        
        # Check for exact phrase match
        if query in content:
            score += 10
        
        # Count individual words
        query_words = query.split()
        for word in query_words:
            if len(word) > 2:  # Skip very short words
                score += content.count(word)
        
        # Also check metadata
        for k, v in chunk['metadata'].items():
            if isinstance(v, str) and query in v.lower():
                score += 5
            elif isinstance(v, str):
                for word in query_words:
                    if len(word) > 2 and word in v.lower():
                        score += 1
        
        if score > 0:
            scored_chunks.append((score, chunk))
    
    # Sort by score (descending)
    scored_chunks.sort(key=lambda x: x[0], reverse=True)
    
    # Return top results
    return [chunk for _, chunk in scored_chunks[:limit]]

def main():
    parser = argparse.ArgumentParser(description='Query ClickHouse documentation chunks')
    parser.add_argument('--pickle', type=str, default='clickhouse_docs_chunks.pkl',
                        help='Path to the pickle file containing document chunks')
    parser.add_argument('--query', type=str, required=True,
                        help='Search query')
    parser.add_argument('--limit', type=int, default=5,
                        help='Maximum number of results to return')
    
    args = parser.parse_args()
    
    # Load the chunks
    chunks = load_chunks(args.pickle)
    print(f"Loaded {len(chunks)} document chunks")
    
    # Perform simple search
    results = simple_search(chunks, args.query, args.limit)
    
    # Display results
    print(f"\nTop {len(results)} results for '{args.query}':\n")
    for i, result in enumerate(results):
        print(f"--- Result {i+1} ---")
        print(f"Document: {result['metadata']['document_title']}")
        print(f"Section: {result['metadata']['section_title']}")
        print(f"Source: {result['metadata']['path']}")
        
        # Print a snippet of the content around the query words
        content = result['content'].lower()
        content_orig = result['content']
        query = args.query.lower()
        
        # First try to find the entire query
        query_pos = content.find(query)
        if query_pos != -1:
            start = max(0, query_pos - 50)
            end = min(len(content), query_pos + len(query) + 50)
            context = content_orig[start:end]
            print(f"Context: ...{context}...")
        else:
            # Try to find any of the query words
            query_words = [w for w in query.split() if len(w) > 2]
            for word in query_words:
                query_pos = content.find(word)
                if query_pos != -1:
                    start = max(0, query_pos - 50)
                    end = min(len(content), query_pos + len(word) + 50)
                    context = content_orig[start:end]
                    print(f"Context: ...{context}...")
                    break
            else:
                # Just show the beginning of the content
                print(f"Context: {content_orig[:100]}...")
        
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