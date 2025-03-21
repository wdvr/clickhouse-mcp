#!/usr/bin/env python3

import pickle
import os
from pathlib import Path
from collections import Counter, defaultdict

from clickhouse_mcp.docs_search import get_default_pickle_path


def load_chunks(pickle_path):
    """Load chunks from a pickle file."""
    with open(pickle_path, 'rb') as f:
        chunks = pickle.load(f)
    return chunks

def analyze_chunks(chunks):
    """Analyze chunks and return various statistics."""
    chunk_lengths = [len(chunk['content']) for chunk in chunks]
    
    # Basic statistics
    stats = {
        'total_chunks': len(chunks),
        'min_length': min(chunk_lengths),
        'max_length': max(chunk_lengths),
        'avg_length': sum(chunk_lengths) / len(chunk_lengths),
        'median_length': sorted(chunk_lengths)[len(chunk_lengths) // 2],
    }
    
    # Count chunks by size ranges
    size_ranges = [(0, 1000), (1000, 2000), (2000, 3000), (3000, 4000), 
                  (4000, 5000), (5000, 6000), (6000, 7000), (7000, 8000),
                  (8000, 9000), (9000, 10000), (10000, float('inf'))]
    
    size_distribution = {f"{r[0]}-{r[1]}": 0 for r in size_ranges}
    for length in chunk_lengths:
        for r in size_ranges:
            if r[0] <= length < r[1]:
                size_distribution[f"{r[0]}-{r[1]}"] += 1
                break

    # Analyze by source file
    chunks_by_source = defaultdict(list)
    for chunk in chunks:
        source = chunk['metadata']['source']
        chunks_by_source[source].append(len(chunk['content']))
    
    source_stats = {}
    for source, lengths in chunks_by_source.items():
        source_stats[source] = {
            'chunks': len(lengths),
            'min_length': min(lengths),
            'max_length': max(lengths),
            'avg_length': sum(lengths) / len(lengths)
        }
    
    # Find largest chunks
    largest_chunks = sorted([(i, len(chunk['content']), chunk['metadata']['source'], 
                             chunk['metadata']['section_title']) 
                           for i, chunk in enumerate(chunks)],
                          key=lambda x: x[1], reverse=True)[:10]
    
    return {
        'stats': stats,
        'size_distribution': size_distribution,
        'source_stats': source_stats,
        'largest_chunks': largest_chunks,
        'all_lengths': chunk_lengths
    }

def main():
    # Load chunks
    pickle_path = get_default_pickle_path()
    chunks = load_chunks(pickle_path)
    
    # Analyze chunks
    analysis = analyze_chunks(chunks)
    
    # Print basic statistics
    print(f"Total chunks: {analysis['stats']['total_chunks']}")
    print(f"Min length: {analysis['stats']['min_length']}")
    print(f"Max length: {analysis['stats']['max_length']}")
    print(f"Avg length: {analysis['stats']['avg_length']:.2f}")
    print(f"Median length: {analysis['stats']['median_length']}")
    
    # Print size distribution
    print("\nSize distribution:")
    for range_label, count in analysis['size_distribution'].items():
        print(f"{range_label}: {count}")
    
    # Print 10 largest chunks
    print("\n10 largest chunks:")
    for i, (chunk_idx, length, source, title) in enumerate(analysis['largest_chunks'], 1):
        print(f"{i}. Length: {length}, Source: {os.path.basename(source)}, Title: {title}")
    
    # Create ASCII histogram
    print("\nASCII Histogram of chunk lengths:")
    max_length = max(analysis['all_lengths'])
    bin_size = max_length // 20
    bins = [0] * 21
    
    for length in analysis['all_lengths']:
        bin_idx = min(length // bin_size, 20)
        bins[bin_idx] += 1
    
    max_bin_count = max(bins)
    scale = min(max_bin_count, 50)
    
    for i, count in enumerate(bins):
        if i == 20:
            range_str = f"{i*bin_size}+"
        else:
            range_str = f"{i*bin_size}-{(i+1)*bin_size}"
        
        bar_len = int((count / max_bin_count) * scale) if max_bin_count > 0 else 0
        bar = '#' * bar_len
        print(f"{range_str.ljust(12)} | {bar} {count}")

if __name__ == "__main__":
    main()