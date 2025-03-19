#!/usr/bin/env python3

import os
import re
import yaml
import pickle
import argparse
from pathlib import Path
from typing import List, Dict, Any, Tuple
from clickhouse_mcp.docs_search import get_project_root


def extract_frontmatter(content: str) -> Tuple[Dict[str, Any], str]:
    """
    Extract YAML frontmatter from the content if present.
    
    Args:
        content: Markdown content to extract frontmatter from.
        
    Returns:
        A tuple of (frontmatter_dict, content_without_frontmatter)
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


def get_docs_dir() -> Path:
    """Get the ClickHouse docs directory path."""
    return get_project_root() / "clickhouse" / "docs" / "en"


def chunk_markdown_by_headers(filepath: str) -> List[Dict[str, Any]]:
    """
    Extracts sections from a markdown file based on h1, h2, and h3 headers.
    
    Args:
        filepath: Path to the markdown file to process.
        
    Returns:
        List of chunks with h1 title appended to each section.
        Handles YAML frontmatter if present.
        Creates a single chunk for files with no sections.
        Each chunk has a unique key in its metadata.
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
        if h1_match:
            h1_title = h1_match.group(1).strip()
        else:
            # If no title found, use the filename as title
            h1_title = Path(filepath).stem.replace('-', ' ').title()
    
    # Find all h2 and h3 sections
    # The pattern looks for ## or ### heading followed by content until the next ## or ### or end of file
    section_pattern = r'(#{2,3}) (.+?)(?=\n#{2,3} |\Z)'
    sections = list(re.finditer(section_pattern, content_without_frontmatter, re.DOTALL))
    
    # Get docs directory for path normalization
    docs_dir = get_docs_dir()
    
    # Try to make path relative to docs directory
    try:
        path = Path(filepath).relative_to(docs_dir)
        path = str(path)
        
        # Create a normalized file path for the chunk key
        # Use a more complete path to avoid key collisions between files with the same name in different directories
        # Remove file extension and convert directory separators to dashes to create a unique filename part
        normalized_path = path.replace('/', '-').replace('\\', '-').replace('.md', '')
    except ValueError:
        # If not under docs directory, use the full path
        path = str(filepath)
        # Use just the filename as key
        normalized_path = Path(filepath).stem
    
    chunks = []
    
    # If there are sections, process them
    if sections:
        # Check if there's non-trivial text before the first section
        first_section_start = content_without_frontmatter.find(sections[0].group(0))
        intro_text = content_without_frontmatter[:first_section_start].strip()
        
        # Create intro chunk if there's significant content before the first section
        if len(intro_text) > 100:  # Arbitrary threshold to filter out short texts
            # Create a summary of sections in the document
            section_titles = []
            for section in sections:
                level = len(section.group(1))  # Number of # characters
                title = section.group(2).strip()
                title_clean = re.sub(r'\{#[\w-]+\}', '', title).strip()
                section_titles.append(f"{'  ' * (level-2)}- {title_clean}")
            
            section_summary = "\n".join(section_titles) if section_titles else ""
            
            # Create intro chunk content
            intro_chunk_text = f"# {h1_title}\n\n{intro_text}\n\n## Sections in this document:\n{section_summary}"
            
            # Generate a unique key for this chunk
            chunk_key = f"{normalized_path}::intro"
            
            # Prepare metadata for intro chunk
            intro_metadata = {
                "source": str(filepath),
                "document_title": h1_title,
                "section_title": "Introduction",
                "path": path,
                "chunk_key": chunk_key  # Add unique key to metadata
            }
            
            # Add relevant frontmatter to metadata
            for key in ['description', 'keywords']:
                if key in frontmatter:
                    intro_metadata[key] = frontmatter[key]
            
            # Add intro chunk
            chunks.append({
                "content": intro_chunk_text,
                "metadata": intro_metadata
            })
        
        # Keep track of section titles to handle duplicates
        section_counts = {}
        
        # Process each section
        for i, section in enumerate(sections):
            section_level = len(section.group(1))  # Number of # characters
            section_header = section.group(1) + " " + section.group(2)
            section_text = section.group(0)
            
            # Extract the section title
            section_title_match = re.match(r'#{2,3} (.+?)(?:\{#[\w-]+\})?(\n|$)', section_text)
            if section_title_match:
                section_title = section_title_match.group(1).strip()
                
                # Clean the section title for use in the key
                clean_section_title = re.sub(r'[^\w\s-]', '', section_title).strip().lower().replace(' ', '-')
                
                # Handle duplicate section titles within the same file
                if clean_section_title in section_counts:
                    section_counts[clean_section_title] += 1
                    # Add a suffix to make the key unique
                    unique_section_title = f"{clean_section_title}-{section_counts[clean_section_title]}"
                else:
                    section_counts[clean_section_title] = 1
                    unique_section_title = clean_section_title
                
                # Generate a unique key for this chunk
                chunk_key = f"{normalized_path}::{unique_section_title}"
                
                # Create a chunk with h1 title incorporated
                chunk_text = f"# {h1_title}: {section_title}\n\n{section_text}"
                
                # Prepare metadata
                metadata = {
                    "source": str(filepath),
                    "document_title": h1_title,
                    "section_title": section_title,
                    "path": path,
                    "section_level": section_level,  # Add section level information
                    "chunk_key": chunk_key  # Add unique key to metadata
                }
                
                # Add relevant frontmatter to metadata
                for key in ['description', 'keywords']:
                    if key in frontmatter:
                        metadata[key] = frontmatter[key]
                
                chunks.append({
                    "content": chunk_text,
                    "metadata": metadata
                })
    
    # If no chunks were created (no sections found), create a single chunk for the whole document
    if not chunks:
        # Generate a unique key for this chunk
        chunk_key = f"{normalized_path}::full"
        
        # Create a chunk with the entire content
        chunk_text = f"# {h1_title}\n\n{content_without_frontmatter}"
        
        # Prepare metadata
        metadata = {
            "source": str(filepath),
            "document_title": h1_title,
            "section_title": "Full Document",
            "path": path,
            "chunk_key": chunk_key  # Add unique key to metadata
        }
        
        # Add relevant frontmatter to metadata
        for key in ['description', 'keywords']:
            if key in frontmatter:
                metadata[key] = frontmatter[key]
        
        chunks.append({
            "content": chunk_text,
            "metadata": metadata
        })
    
    return chunks


def process_directory(directory_path: str) -> List[Dict[str, Any]]:
    """
    Process all markdown files in a directory and its subdirectories.
    
    Args:
        directory_path: Path to the directory containing markdown files.
        
    Returns:
        List of all chunks from all files.
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


def save_chunks_to_pickle(chunks: List[Dict[str, Any]], output_file: str) -> None:
    """
    Save chunks to a pickle file for later use.
    
    Args:
        chunks: List of document chunks to save.
        output_file: Path to the output pickle file.
    """
    with open(output_file, 'wb') as f:
        pickle.dump(chunks, f)
    print(f"Saved {len(chunks)} chunks to {output_file}")


def get_default_output_path() -> Path:
    """Get the default path for the output pickle file."""
    return get_project_root() / "index" / "clickhouse_docs_chunks.pkl"


def get_default_docs_path() -> Path:
    """Get the default path for the documentation directory."""
    return get_docs_dir() / "sql-reference"


def main():
    parser = argparse.ArgumentParser(description='Chunk markdown files by headers for use with langchain.')
    
    # Get default paths
    default_docs_dir = get_default_docs_path()
    default_output_file = get_default_output_path()
    
    parser.add_argument('--dir', type=str, default=str(default_docs_dir),
                        help='Directory containing markdown files to process')
    parser.add_argument('--output', type=str, default=str(default_output_file),
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
            print(f"Document Title: {chunk['metadata']['document_title']}")
            print(f"Section Title: {chunk['metadata']['section_title']}")
            print(f"Chunk Key: {chunk['metadata']['chunk_key']}")
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