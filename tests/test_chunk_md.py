#!/usr/bin/env python3

import sys
import os
import unittest
import pickle
import random
from pathlib import Path

# Add the tools directory to the path so we can import chunk_md
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tools"))
import chunk_md

class TestChunkMarkdown(unittest.TestCase):

    def setUp(self):
        # Path to the test data directory
        self.docs_dir = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.syntax_md_path = self.docs_dir / "clickhouse" / "docs" / "en" / "sql-reference" / "syntax.md"
        self.pickle_path = chunk_md.get_default_output_path()
        
        # Load the pickled chunks for comparison
        with open(self.pickle_path, 'rb') as f:
            self.chunks = pickle.load(f)

    def test_syntax_md_chunking(self):
        """Test that the syntax.md file is correctly chunked."""
        # Process the syntax.md file
        chunks = chunk_md.chunk_markdown_by_headers(self.syntax_md_path)
        
        # Verify the chunks match expected structure
        self.assertGreater(len(chunks), 0, "Should have produced chunks")
        
        # Find chunks corresponding to syntax.md in the pickle file
        syntax_chunks = [c for c in self.chunks if c['metadata']['path'] == "sql-reference/syntax.md"]
        
        # Check that we have the expected sections (these should be in the chunked file)
        expected_sections = [
            "Query Parsing",
            "Spaces",
            "Comments", 
            "Keywords",
            "Identifiers",
            "Literals",
            "Defining and Using Query Parameters",
            "Functions",
            "Operators",
            "Data Types and Database Table Engines",
            "Expressions",
            "Expression Aliases",
            "Asterisk"
        ]
        
        # Get all section titles from syntax chunks
        actual_sections = [chunk['metadata']['section_title'] for chunk in syntax_chunks]
        
        # Verify all expected sections exist
        for section in expected_sections:
            self.assertIn(section, actual_sections, f"Missing section: {section}")
            
        # Since we now handle more headers (H3) and intro sections, we don't require an exact match
        # Instead, we ensure that the number of chunks is at least the expected number
        self.assertGreaterEqual(len(syntax_chunks), len(expected_sections), 
                              f"Expected at least {len(expected_sections)} chunks, got {len(syntax_chunks)}")
        
        # Check that each chunk has a unique key
        chunk_keys = [chunk['metadata']['chunk_key'] for chunk in syntax_chunks]
        self.assertEqual(len(chunk_keys), len(set(chunk_keys)), 
                         "All chunk keys should be unique")

    def test_specific_chunk_content(self):
        """Test specific chunks to verify content and metadata."""
        # Find the "Literals" section from syntax.md
        literals_chunks = [c for c in self.chunks 
                          if c['metadata']['path'] == "sql-reference/syntax.md" 
                          and c['metadata']['section_title'] == "Literals"]
        
        self.assertEqual(len(literals_chunks), 1, "Should find exactly one Literals section")
        literals_chunk = literals_chunks[0]
        
        # Check metadata
        self.assertEqual(literals_chunk['metadata']['document_title'], "Syntax")
        # Check for description if it exists, but don't require it
        if 'description' in literals_chunk['metadata']:
            self.assertEqual(literals_chunk['metadata']['description'], "Documentation for Syntax")
        
        # Check for chunk_key which should now be present
        self.assertTrue('chunk_key' in literals_chunk['metadata'], "Chunk should have a chunk_key")
        # We now include directory paths in the keys, so the key format is something like "sql-reference-syntax::literals"
        self.assertTrue(literals_chunk['metadata']['chunk_key'].endswith("::literals"), 
                      f"Chunk key '{literals_chunk['metadata']['chunk_key']}' should end with '::literals'")
        
        # Verify content
        self.assertIn("## Literals", literals_chunk['content'])
        self.assertIn("In ClickHouse, a literal is a value which is directly represented in a query", 
                      literals_chunk['content'])
        self.assertIn("String", literals_chunk['content'])
        self.assertIn("Numeric", literals_chunk['content'])
        self.assertIn("Compound", literals_chunk['content'])
        self.assertIn("NULL", literals_chunk['content'])
        
        # Check another section like "Comments"
        comments_chunks = [c for c in self.chunks 
                          if c['metadata']['path'] == "sql-reference/syntax.md" 
                          and c['metadata']['section_title'] == "Comments"]
        
        self.assertEqual(len(comments_chunks), 1, "Should find exactly one Comments section")
        comments_chunk = comments_chunks[0]
        
        # Check for chunk_key for comments section
        self.assertTrue('chunk_key' in comments_chunk['metadata'], "Chunk should have a chunk_key")
        # We now include directory paths in the keys, so the key format is something like "sql-reference-syntax::comments"
        self.assertTrue(comments_chunk['metadata']['chunk_key'].endswith("::comments"), 
                      f"Chunk key '{comments_chunk['metadata']['chunk_key']}' should end with '::comments'")
        
        # Verify content for Comments section
        self.assertIn("## Comments", comments_chunk['content'])
        self.assertIn("ClickHouse supports both SQL-style and C-style comments", comments_chunk['content'])
        self.assertIn("SQL-style comments begin with", comments_chunk['content'])
        self.assertIn("C-style comments span from", comments_chunk['content'])
        
    def test_previously_empty_files(self):
        """Test that files which previously had zero chunks now have at least one chunk."""
        # Find a file that would have previously had zero chunks - files with only H1 headers
        # Let's test the index.md file which typically only has an H1 header and some content
        index_md_path = self.docs_dir / "clickhouse" / "docs" / "en" / "sql-reference" / "index.md"
        
        # Process the index.md file
        chunks = chunk_md.chunk_markdown_by_headers(index_md_path)
        
        # There should be at least one chunk even if there are no H2/H3 headers
        self.assertGreaterEqual(len(chunks), 1, "A file with only H1 headers should still produce at least one chunk")
        
        # The chunk should have appropriate metadata with a key
        chunk = chunks[0]
        self.assertTrue('chunk_key' in chunk['metadata'], "Chunk should have a chunk_key")
        
        # For a file with no H2/H3 sections, we expect a 'full' chunk
        self.assertTrue(chunk['metadata']['chunk_key'].endswith("::full"), 
                        "A file with no sections should have a key ending with '::full'")
        
        # The content should contain the content of the file
        self.assertGreater(len(chunk['content']), 50, "Chunk should contain meaningful content")
        
        # Let's also check the actual documents in the pickled chunks
        index_chunks = [c for c in self.chunks if c['metadata']['path'] == "sql-reference/index.md"]
        self.assertGreaterEqual(len(index_chunks), 1, "The index.md file should have at least one chunk in the pickle file")
    
    def test_unique_keys_across_all_chunks(self):
        """Test that all chunks have unique keys across the entire dataset."""
        # Get all chunk keys from the pickle file
        all_chunk_keys = [chunk['metadata']['chunk_key'] for chunk in self.chunks]
        
        # Check that all chunks have a key
        self.assertEqual(len(all_chunk_keys), len(self.chunks), "Every chunk should have a key")
        
        # Check for uniqueness across all chunks
        unique_keys = set(all_chunk_keys)
        self.assertEqual(len(unique_keys), len(all_chunk_keys), 
                        f"Expected {len(all_chunk_keys)} unique keys, but found {len(unique_keys)}")
        
        # Check the format of keys (should be something like "path-to-file::section")
        # Sample a few keys to verify format
        sample_keys = random.sample(all_chunk_keys, min(10, len(all_chunk_keys)))
        for key in sample_keys:
            # Our keys now include directory paths, so they can be more complex
            self.assertRegex(key, r'^[a-zA-Z0-9_-]+(-[a-zA-Z0-9_-]+)*::[a-zA-Z0-9_-]+(-[a-zA-Z0-9_-]+)*(-\d+)?$', 
                            f"Key '{key}' should follow the expected format")
            
        # Verify different types of chunks have correct suffixes
        intro_keys = [k for k in all_chunk_keys if k.endswith('::intro')]
        full_keys = [k for k in all_chunk_keys if k.endswith('::full')]
        
        self.assertGreater(len(intro_keys), 0, "There should be some intro chunks")
        self.assertGreater(len(full_keys), 0, "There should be some full document chunks")


if __name__ == "__main__":
    unittest.main()