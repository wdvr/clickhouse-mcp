#!/usr/bin/env python3

import os
import re
import yaml
import pickle
import argparse
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from clickhouse_mcp.docs_search import get_project_root

def extract_frontmatter(content: str) -> Tuple[Dict[str, Any], str]:
    """
    Extract YAML frontmatter from the content if present.
    
    Args:
        content: Markdown content to extract frontmatter from
        
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
    docs_path = get_project_root() / "clickhouse_docs"
    
    if not docs_path.exists():
        print("ClickHouse docs not found. Running checkout script...")
        import subprocess
        import sys
        
        checkout_script = get_project_root() / "tools" / "checkout_clickhouse_docs.py"
        result = subprocess.run([sys.executable, str(checkout_script)], check=False)
        
        if result.returncode != 0:
            print("Failed to checkout ClickHouse docs. Please run tools/checkout_clickhouse_docs.py manually.")
            sys.exit(1)
    
    return docs_path


def find_headers(content: str, header_level: int) -> List[Dict[str, Any]]:
    """
    Find all headers of specified level (e.g., ## for H2, ### for H3) in content.
    
    Args:
        content: Markdown content to search
        header_level: The header level to find (2 for H2, 3 for H3)
        
    Returns:
        List of sections with title and content
    """
    marker = '#' * header_level
    pattern = f'(?m)^{marker} '
    
    # Split content by headers of specified level
    parts = re.split(pattern, content)
    
    sections = []
    
    # First part is content before any header of this level
    intro = parts[0].strip()
    if intro:
        sections.append({
            "title": "Introduction",
            "content": intro
        })
    
    # Process each header section
    for part in parts[1:]:
        # Extract the title (first line) and the rest
        lines = part.split('\n', 1)
        if len(lines) > 1:
            title = lines[0].strip()
            content = f"{marker} {title}\n{lines[1]}"  # Reconstruct with header
            sections.append({
                "title": title,
                "content": content
            })
        elif part.strip():  # Handle case of a header with no content
            title = part.strip()
            sections.append({
                "title": title,
                "content": f"{marker} {title}"
            })
    
    return sections


def get_section_title_from_content(content: str) -> str:
    """Extract a title from content based on headers."""
    # Try to extract title from H2
    h2_match = re.search(r'^## ([^\n]+)', content)
    if h2_match:
        return h2_match.group(1).strip()
    
    # Try to extract title from H3
    h3_match = re.search(r'^### ([^\n]+)', content)
    if h3_match:
        return h3_match.group(1).strip()
    
    # Default
    return "Section"


def count_paragraphs(content: str) -> int:
    """Count paragraphs that actually have content."""
    paragraphs = re.split(r'\n\s*\n', content)
    return sum(1 for p in paragraphs if p.strip())


def build_chunk_key(base_path: str, section_path: List[str]) -> str:
    """
    Build a chunk key from base path and section path.
    
    Args:
        base_path: Base path for the document
        section_path: List of section titles in the hierarchy
        
    Returns:
        A clean and reasonably sized chunk key
    """
    # Take at most 3 section levels to avoid excessively long keys
    used_sections = section_path[:3]
    
    if not used_sections:
        return base_path
    
    # Clean sections for use in keys
    cleaned_sections = []
    for section in used_sections:
        # Remove non-word characters and replace spaces with hyphens
        clean = re.sub(r'[^\w\s-]', '', section).strip().lower().replace(' ', '-')
        if clean:
            cleaned_sections.append(clean)
    
    # Join the base path with section path
    if cleaned_sections:
        return f"{base_path}::{'-'.join(cleaned_sections)}"
    else:
        return base_path


def split_by_natural_breaks(content: str, target_size: int) -> List[str]:
    """
    Split content by natural breaks (paragraphs, then sentences, then words).
    
    Args:
        content: Content to split
        target_size: Target size for each chunk
        
    Returns:
        List of content chunks
    """
    # First try to split by paragraphs
    paragraphs = re.split(r'\n\s*\n', content)
    paragraphs = [p for p in paragraphs if p.strip()]
    
    # If we have multiple paragraphs, use them for chunking
    if len(paragraphs) > 1:
        return group_elements_by_size(paragraphs, target_size, separator="\n\n")
    
    # If we have very few paragraphs or extremely long ones, try to split by lines
    lines = content.split('\n')
    lines = [line for line in lines if line.strip()]
    if len(lines) > 1:
        return group_elements_by_size(lines, target_size, separator="\n")
    
    # If we still have very long content, split by sentences
    sentences = re.split(r'(?<=[.!?])\s+', content)
    sentences = [s for s in sentences if s.strip()]
    if len(sentences) > 1:
        return group_elements_by_size(sentences, target_size, separator=" ")
    
    # Next resort: split by words
    words = content.split()
    if len(words) > 1:
        return group_elements_by_size(words, target_size, separator=" ")
    
    # Handle very long content with no natural breaks (like long code blocks, large polygons)
    # Force splitting into roughly equal chunks of target_size characters
    chunks = []
    
    # If the content is extremely large, we need to force smaller chunks
    max_chunk_size = min(target_size, 5000)  # Never exceed 5000 chars per chunk
    
    # Ensure we're creating multiple chunks when content is large
    if len(content) > max_chunk_size:
        chunk_size = max(500, max_chunk_size)  # Minimum chunk of 500 chars
        
        for i in range(0, len(content), chunk_size):
            # Try to find a good break point (space, comma, etc.) near the target size
            end_pos = min(i + chunk_size, len(content))
            
            # If not at the end of content and not at a natural break, look for a better break point
            if end_pos < len(content) and end_pos > i + chunk_size // 2:
                # Look for spaces, commas, etc. to break at
                for break_char in [' ', ',', '.', '\n', ')', '}', ']']:
                    # Search for the break character in the latter half of the chunk
                    search_start = i + chunk_size // 2
                    pos = content.rfind(break_char, search_start, end_pos)
                    if pos > search_start:
                        end_pos = pos + 1  # Include the break character
                        break
            
            chunk = content[i:end_pos]
            if chunk.strip():  # Only add non-empty chunks
                chunks.append(chunk)
    else:
        chunks = [content]
    
    return chunks


def group_elements_by_size(elements: List[str], target_size: int, separator: str) -> List[str]:
    """
    Group elements into chunks close to target_size.
    
    Args:
        elements: List of content elements (paragraphs, lines, etc.)
        target_size: Target size for each group
        separator: String to use when joining elements
        
    Returns:
        List of content chunks
    """
    chunks = []
    current_chunk = []
    current_size = 0
    
    # Maximum size for any single chunk
    max_chunk_size = min(target_size * 1.5, 5000)  # Never exceed 5000 chars
    
    for element in elements:
        element_size = len(element) + len(separator) if current_chunk else len(element)
        
        # If element itself is larger than target size, we need to split it further
        if element_size > max_chunk_size and len(element) > 100:
            # If we have accumulated content, finalize current chunk
            if current_chunk:
                chunks.append(separator.join(current_chunk))
                current_chunk = []
                current_size = 0
                
            # For extremely large elements, split them into smaller chunks
            # This handles cases like very long code blocks or polygon definitions
            if len(element) > max_chunk_size:
                # Force split into smaller chunks
                split_size = max(500, int(max_chunk_size * 0.8))  # 80% of max size
                
                # Try to find break points at reasonable boundaries
                start = 0
                while start < len(element):
                    # Target end position
                    end = min(start + split_size, len(element))
                    
                    # If not at the end, look for a better break point
                    if end < len(element) and end > start + split_size // 2:
                        # Try to break at spaces, punctuation, etc.
                        for break_char in ['\n', ' ', ',', '.', ')', '}', ']', ';']:
                            pos = element.rfind(break_char, start + split_size // 2, end)
                            if pos > start + split_size // 2:
                                end = pos + 1  # Include the break character
                                break
                    
                    # Extract the chunk and add it
                    chunk = element[start:end]
                    if chunk.strip():
                        chunks.append(chunk)
                    
                    start = end
            else:
                # For moderately large elements, add them as a single chunk
                chunks.append(element)
            
            continue
        
        # If adding this element would exceed target size and we already have content,
        # finalize current chunk and start a new one
        if current_size + element_size > target_size and current_chunk:
            chunks.append(separator.join(current_chunk))
            
            # Start new chunk
            current_chunk = [element]
            current_size = len(element)
        else:
            current_chunk.append(element)
            current_size += element_size
    
    # Add the last chunk if there's anything left
    if current_chunk:
        chunks.append(separator.join(current_chunk))
    
    return chunks




def process_markdown_document(
    content: str,
    file_path: str,
    target_size: int = 5000
) -> List[Dict[str, Any]]:
    """
    Process a markdown document, extracting frontmatter and chunking content.
    
    Args:
        content: Markdown content to process
        file_path: Path to the markdown file
        target_size: Target size for chunks
        
    Returns:
        List of chunks with metadata
    """
    # Extract frontmatter
    frontmatter, content_without_frontmatter = extract_frontmatter(content)
    
    # Get document title
    if 'title' in frontmatter:
        document_title = frontmatter['title']
    else:
        h1_match = re.search(r'# (.+?)(\n|$)', content_without_frontmatter)
        if h1_match:
            document_title = h1_match.group(1).strip()
        else:
            document_title = Path(file_path).stem.replace('-', ' ').title()
    
    # Get normalized path for chunk keys
    docs_dir = get_docs_dir()
    try:
        rel_path = Path(file_path).relative_to(docs_dir)
        path = str(rel_path)
        normalized_path = path.replace('/', '-').replace('\\', '-').replace('.md', '')
    except ValueError:
        path = str(file_path)
        normalized_path = Path(file_path).stem
    
    # Start the chunking process with the top-level document
    return process_document_sections(
        content_without_frontmatter,
        file_path,
        normalized_path,
        document_title,
        frontmatter,
        target_size
    )


def process_document_sections(
    content: str,
    file_path: str,
    normalized_path: str,
    document_title: str,
    frontmatter: Dict[str, Any],
    target_size: int
) -> List[Dict[str, Any]]:
    """
    Process a document by first chunking by headers and then by content.
    
    Args:
        content: Content to process
        file_path: Path to the file
        normalized_path: Normalized path for chunk keys
        document_title: Document title
        frontmatter: Document frontmatter
        target_size: Target size for chunks
        
    Returns:
        List of chunks with metadata
    """
    # First try splitting by H2 headers
    h2_sections = find_headers(content, 2)
    
    # If we have H2 headers, process each section
    if len(h2_sections) > 1 or (len(h2_sections) == 1 and h2_sections[0]["title"] != "Introduction"):
        return process_header_sections(
            h2_sections, 
            file_path, 
            normalized_path, 
            document_title, 
            frontmatter, 
            target_size,
            header_level=2
        )
    
    # If no H2 headers, try H3 headers
    h3_sections = find_headers(content, 3)
    
    # If we have H3 headers, process each section
    if len(h3_sections) > 1 or (len(h3_sections) == 1 and h3_sections[0]["title"] != "Introduction"):
        return process_header_sections(
            h3_sections, 
            file_path, 
            normalized_path, 
            document_title, 
            frontmatter, 
            target_size,
            header_level=3
        )
    
    # No headers, chunk directly by content
    return chunk_by_content(
        content, 
        file_path, 
        normalized_path, 
        document_title, 
        [], 
        frontmatter, 
        target_size
    )


def process_header_sections(
    sections: List[Dict[str, Any]],
    file_path: str,
    normalized_path: str,
    document_title: str,
    frontmatter: Dict[str, Any],
    target_size: int,
    header_level: int
) -> List[Dict[str, Any]]:
    """
    Process sections divided by headers, recursively handling subsections.
    
    Args:
        sections: List of sections with titles and content
        file_path: Path to the file
        normalized_path: Normalized path for chunk keys
        document_title: Document title
        frontmatter: Document frontmatter
        target_size: Target size for chunks
        header_level: Current header level (2 for H2, 3 for H3)
        
    Returns:
        List of chunks with metadata
    """
    all_chunks = []
    section_counts = {}
    
    # Process introduction if it exists
    if sections and sections[0]["title"] == "Introduction":
        intro_section = sections[0]
        intro_content = intro_section["content"]
        
        # Create section overview if there are other sections
        if len(sections) > 1:
            section_titles = [section["title"] for section in sections[1:]]
            section_summary = "\n".join([f"- {title}" for title in section_titles])
            intro_content += f"\n\n## Sections in this document:\n{section_summary}"
        
        # Chunk the introduction
        intro_chunks = chunk_by_content(
            intro_content,
            file_path,
            normalized_path,
            document_title,
            ["Introduction"],
            frontmatter,
            target_size
        )
        all_chunks.extend(intro_chunks)
        
        # Skip the introduction in the main processing loop
        remaining_sections = sections[1:]
    else:
        remaining_sections = sections
    
    # Process each section
    for section in remaining_sections:
        section_title = section["title"]
        section_content = section["content"]
        
        # Handle duplicate section titles
        if section_title in section_counts:
            section_counts[section_title] += 1
            unique_section_title = f"{section_title} ({section_counts[section_title]})"
        else:
            section_counts[section_title] = 1
            unique_section_title = section_title
        
        # Section path for this section
        section_path = [unique_section_title]
        
        # Process this section based on its structure
        # For H2 sections, check for H3 subsections
        if header_level == 2:
            h3_sections = find_headers(section_content, 3)
            
            if len(h3_sections) > 1 or (len(h3_sections) == 1 and h3_sections[0]["title"] != "Introduction"):
                # Process H3 subsections
                section_chunks = process_header_sections(
                    h3_sections,
                    file_path,
                    normalized_path,
                    document_title,
                    frontmatter,
                    target_size,
                    header_level=3
                )
                
                # Update section path for all chunks in this section
                for chunk in section_chunks:
                    updated_path = [unique_section_title] + chunk["metadata"].get("section_path", [])
                    chunk["metadata"]["section_path"] = updated_path
                    # Update chunk key
                    chunk["metadata"]["chunk_key"] = build_chunk_key(normalized_path, updated_path)
            else:
                # No H3 subsections, chunk directly
                section_chunks = chunk_by_content(
                    section_content,
                    file_path,
                    normalized_path,
                    document_title,
                    [unique_section_title],
                    frontmatter,
                    target_size
                )
        else:
            # For H3 sections, just chunk directly
            section_chunks = chunk_by_content(
                section_content,
                file_path,
                normalized_path,
                document_title,
                [unique_section_title],
                frontmatter,
                target_size
            )
        
        all_chunks.extend(section_chunks)
    
    # Add navigation links between chunks
    add_navigation_links(all_chunks)
    return all_chunks


def chunk_by_content(
    content: str,
    file_path: str,
    normalized_path: str,
    document_title: str,
    section_path: List[str],
    frontmatter: Dict[str, Any],
    target_size: int
) -> List[Dict[str, Any]]:
    """
    Chunk content by natural breaks and create chunk objects.
    
    Args:
        content: Content to chunk
        file_path: Path to the file
        normalized_path: Normalized path for chunk keys
        document_title: Document title
        section_path: Section path for this content
        frontmatter: Document frontmatter
        target_size: Target size for chunks
        
    Returns:
        List of chunks with metadata
    """
    # Split content into chunks
    content_pieces = split_by_natural_breaks(content, target_size)
    
    # Create chunk objects
    chunks = []
    for i, piece_content in enumerate(content_pieces, 1):
        # If multiple chunks, add part numbers to section path
        if len(content_pieces) > 1:
            part_section_path = section_path + [f"Part {i}"]
        else:
            part_section_path = section_path
        
        # Section title is the last part of the section path, or extract from content
        if part_section_path:
            section_title = part_section_path[-1]
        else:
            section_title = get_section_title_from_content(piece_content)
        
        # Create chunk key
        chunk_key = build_chunk_key(normalized_path, part_section_path)
        
        # Metadata for this chunk
        metadata = {
            "source": file_path,
            "document_title": document_title,
            "section_title": section_title,
            "path": file_path,
            "chunk_key": chunk_key,
            "section_path": part_section_path
        }
        
        # Add relevant frontmatter
        for key in ['description', 'keywords']:
            if key in frontmatter:
                metadata[key] = frontmatter[key]
        
        # Final content with document title
        final_content = f"# {document_title}\n\n{piece_content}"
        
        chunks.append({
            "content": final_content,
            "metadata": metadata
        })
    
    # Add navigation links
    add_navigation_links(chunks)
    return chunks


def add_navigation_links(chunks: List[Dict[str, Any]]) -> None:
    """
    Add navigation links between chunks.
    
    Args:
        chunks: List of chunk objects
    """
    for i, chunk in enumerate(chunks):
        if i > 0:
            chunk["metadata"]["prev_chunk_key"] = chunks[i-1]["metadata"]["chunk_key"]
        if i < len(chunks) - 1:
            chunk["metadata"]["next_chunk_key"] = chunks[i+1]["metadata"]["chunk_key"]


def chunk_markdown_file(filepath: str, target_size: int = 5000) -> List[Dict[str, Any]]:
    """
    Process a markdown file, splitting it into chunks.
    
    Args:
        filepath: Path to the markdown file
        target_size: Target size for chunks in characters
        
    Returns:
        List of chunks with metadata
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    return process_markdown_document(content, str(filepath), target_size)


def process_directory(directory_path: str, target_size: int = 5000) -> List[Dict[str, Any]]:
    """
    Process all markdown files in a directory and its subdirectories.
    
    Args:
        directory_path: Path to the directory containing markdown files
        target_size: Target size for chunks in characters
        
    Returns:
        List of all chunks from all files
    """
    all_chunks = []
    
    # Check if directory_path is a file
    if os.path.isfile(directory_path):
        if directory_path.endswith('.md'):
            try:
                print(f"Processing file: {directory_path}")
                chunks = chunk_markdown_file(directory_path, target_size)
                all_chunks.extend(chunks)
                print(f"Processed {directory_path}: {len(chunks)} chunks extracted")
            except Exception as e:
                print(f"Error processing {directory_path}: {e}")
                import traceback
                traceback.print_exc()
        return all_chunks
    
    # Process directory
    for root, _, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.md'):
                filepath = os.path.join(root, file)
                try:
                    chunks = chunk_markdown_file(filepath, target_size)
                    all_chunks.extend(chunks)
                    print(f"Processed {filepath}: {len(chunks)} chunks extracted")
                except Exception as e:
                    print(f"Error processing {filepath}: {e}")
                    import traceback
                    traceback.print_exc()
    
    return all_chunks


def save_chunks_to_pickle(chunks: List[Dict[str, Any]], output_file: str) -> None:
    """
    Save chunks to a pickle file for later use.
    
    Args:
        chunks: List of document chunks to save
        output_file: Path to the output pickle file
    """
    with open(output_file, 'wb') as f:
        pickle.dump(chunks, f)
    print(f"Saved {len(chunks)} chunks to {output_file}")


def get_default_output_path() -> Path:
    """Get the default path for the output pickle file."""
    return get_project_root() / "src" / "clickhouse_mcp" / "index" / "clickhouse_docs_chunks.pkl"


def get_default_docs_path() -> Path:
    """Get the default path for the documentation directory."""
    return get_docs_dir() / "docs" / "en" / "sql-reference"


def main():
    parser = argparse.ArgumentParser(description='Chunk markdown files for use with vector search.')
    
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
    parser.add_argument('--page-size', type=int, default=5000,
                        help='Target page size in characters (default: 5000)')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.dir):
        print(f"Error: Directory {args.dir} does not exist")
        exit(1)
        
    # Process the directory and get all chunks
    chunks = process_directory(args.dir, args.page_size)
    
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
        
    print("\nThese chunks are ready to be used with vector search, for example:")
    print("import pickle")
    print("from langchain_community.vectorstores import FAISS")
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