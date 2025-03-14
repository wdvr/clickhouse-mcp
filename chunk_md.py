#!/usr/bin/env python3

import os
import re
import yaml
from pathlib import Path


def extract_frontmatter(content):
    """
    Extract YAML frontmatter from the content if present.
    Returns a tuple of (frontmatter_dict, content_without_frontmatter)
    """
    frontmatter = {}
    content_without_frontmatter = content
    
    # Check for YAML frontmatter (between --- markers)
    fm_match = re.match(r'^---\s*\n(.*?)\n---\s*\n', content, re.DOTALL)
    if fm_match:
        try:
            frontmatter = yaml.safe_load(fm_match.group(1))
            content_without_frontmatter = content[fm_match.end():]
        except Exception as e:
            print(f"Warning: Failed to parse frontmatter: {e}")
    
    return frontmatter, content_without_frontmatter


def chunk_markdown_by_headers(filepath):
    """
    Extracts sections from a markdown file based on h1 and h2 headers.
    Returns a list of chunks with h1 title appended to each h2 section.
    Handles YAML frontmatter if present.
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Extract frontmatter if present
    frontmatter, content_without_frontmatter = extract_frontmatter(content)
    
    # Use title from frontmatter if available, otherwise look for h1 header
    if 'title' in frontmatter:
        h1_title = frontmatter['title']
    else:
        h1_match = re.search(r'# (.+?)(\n|$)', content_without_frontmatter)
        if not h1_match:
            return []  # No title found
        h1_title = h1_match.group(1).strip()
    
    # Find all h2 sections
    # The pattern looks for ## heading followed by content until the next ## or end of file
    h2_sections = re.finditer(r'## (.+?)(?=\n## |\Z)', content_without_frontmatter, re.DOTALL)
    
    chunks = []
    for section in h2_sections:
        section_text = section.group(0)
        # Extract the h2 title
        h2_title_match = re.match(r'## (.+?)(?:\{#[\w-]+\})?(\n|$)', section_text)
        if h2_title_match:
            h2_title = h2_title_match.group(1).strip()
            
            # Create a chunk with h1 title incorporated
            chunk_text = f"# {h1_title}: {h2_title}\n\n{section_text}"
            
            # Prepare metadata
            metadata = {
                "source": str(filepath),
                "document_title": h1_title,
                "section_title": h2_title,
                "path": str(filepath).replace("/Users/ivanzaitsev/clickhouse-mcp/clickhouse/docs/en/", "")
            }
            
            # Add relevant frontmatter to metadata
            for key in ['description', 'slug', 'sidebar_label']:
                if key in frontmatter:
                    metadata[key] = frontmatter[key]
            
            chunks.append({
                "content": chunk_text,
                "metadata": metadata
            })
    
    return chunks


def process_directory(directory_path):
    """
    Process all markdown files in a directory and its subdirectories.
    Returns a list of all chunks from all files.
    """
    all_chunks = []
    
    for root, _, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.md'):
                filepath = os.path.join(root, file)
                chunks = chunk_markdown_by_headers(filepath)
                all_chunks.extend(chunks)
                print(f"Processed {filepath}: {len(chunks)} chunks extracted")
    
    return all_chunks


def save_chunks_to_pickle(chunks, output_file):
    """
    Save chunks to a pickle file for later use with langchain.
    """
    import pickle
    with open(output_file, 'wb') as f:
        pickle.dump(chunks, f)
    print(f"Saved {len(chunks)} chunks to {output_file}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Chunk markdown files by headers for use with langchain.')
    parser.add_argument('--dir', type=str, default="/Users/ivanzaitsev/clickhouse-mcp/clickhouse/docs/en/sql-reference",
                        help='Directory containing markdown files to process')
    parser.add_argument('--output', type=str, default="clickhouse_docs_chunks.pkl",
                        help='Output file to save the chunks (pickle format)')
    parser.add_argument('--save', action='store_true', 
                        help='Save chunks to pickle file')
    parser.add_argument('--preview', action='store_true',
                        help='Show preview of the first few chunks')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.dir):
        print(f"Error: Directory {args.dir} does not exist")
        exit(1)
        
    # Process the directory and get all chunks
    chunks = process_directory(args.dir)
    
    print(f"\nTotal chunks extracted: {len(chunks)}")
    
    if args.preview:
        # Show preview of the first few chunks
        print("\nExample of first few chunks:")
        for i, chunk in enumerate(chunks[:3]):
            print(f"\n--- Chunk {i+1} ---")
            print(f"Source: {chunk['metadata']['source']}")
            print(f"Document Title: {chunk['metadata']['document_title']}")
            print(f"Section Title: {chunk['metadata']['section_title']}")
            print(f"Content Preview: {chunk['content'][:150]}...\n")
    
    if args.save:
        save_chunks_to_pickle(chunks, args.output)
        
    print("\nThese chunks are ready to be used with langchain, for example:")
    print("import pickle")
    print("from langchain.vectorstores import FAISS")
    print("from langchain.embeddings import OpenAIEmbeddings")
    print("")
    print("# Load the chunks")
    print(f"with open('{args.output}', 'rb') as f:")
    print("    chunks = pickle.load(f)")
    print("")
    print("# Create embeddings")
    print("embeddings = OpenAIEmbeddings()")
    print("")
    print("# Create vector store")
    print("vector_index = FAISS.from_documents(chunks, embeddings)")


if __name__ == "__main__":
    main()