#!/usr/bin/env python3

import sys
import os
import unittest
import pickle
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
        
        # Check that we have the expected sections
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
            
        # Check that the number of chunks matches the expected count
        self.assertEqual(len(syntax_chunks), len(expected_sections), 
                         f"Expected {len(expected_sections)} chunks, got {len(syntax_chunks)}")

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
        self.assertEqual(literals_chunk['metadata']['description'], "Documentation for Syntax")
        self.assertEqual(literals_chunk['metadata']['slug'], "/sql-reference/syntax")
        
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
        
        # Verify content for Comments section
        self.assertIn("## Comments", comments_chunk['content'])
        self.assertIn("ClickHouse supports both SQL-style and C-style comments", comments_chunk['content'])
        self.assertIn("SQL-style comments begin with", comments_chunk['content'])
        self.assertIn("C-style comments span from", comments_chunk['content'])


if __name__ == "__main__":
    unittest.main()