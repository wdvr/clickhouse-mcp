#!/usr/bin/env python3

import sys
import os
import unittest
from pathlib import Path

# Add the tools directory to the path so we can import chunk_md
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tools"))
import chunk_md


class TestChunkImplementation(unittest.TestCase):
    """Test the core chunking functionality directly."""
    
    def test_split_by_natural_breaks_paragraphs(self):
        """Test splitting content by paragraphs."""
        # Content with multiple paragraphs
        content = "Paragraph 1.\n\nParagraph 2.\n\nParagraph 3."
        chunks = chunk_md.split_by_natural_breaks(content, 15)
        
        # Should split into 3 chunks
        self.assertEqual(len(chunks), 3, "Should split into 3 paragraphs")
        self.assertEqual(chunks[0], "Paragraph 1.")
        self.assertEqual(chunks[1], "Paragraph 2.")
        self.assertEqual(chunks[2], "Paragraph 3.")
        
    def test_split_by_natural_breaks_lines(self):
        """Test splitting content by lines when paragraphs are too large."""
        # Content with one paragraph but multiple lines
        content = "Line 1.\nLine 2.\nLine 3."
        chunks = chunk_md.split_by_natural_breaks(content, 10)
        
        # Should split into individual lines
        self.assertEqual(len(chunks), 3, "Should split into 3 lines")
        self.assertEqual(chunks[0], "Line 1.")
        self.assertEqual(chunks[1], "Line 2.")
        self.assertEqual(chunks[2], "Line 3.")
        
    def test_split_by_natural_breaks_sentences(self):
        """Test splitting content by sentences when lines are too large."""
        # Content with one line but multiple sentences
        content = "Sentence 1. Sentence 2. Sentence 3."
        chunks = chunk_md.split_by_natural_breaks(content, 15)
        
        # Should split into individual sentences
        self.assertEqual(len(chunks), 3, "Should split into 3 sentences")
        self.assertEqual(chunks[0], "Sentence 1.")
        self.assertEqual(chunks[1], "Sentence 2.")
        self.assertEqual(chunks[2], "Sentence 3.")
        
    def test_split_by_natural_breaks_words(self):
        """Test splitting content by words when sentences are too large."""
        # Content with one sentence but multiple words
        content = "This is a very long single sentence without punctuation"
        chunks = chunk_md.split_by_natural_breaks(content, 10)
        
        # Should split into multiple word groups
        self.assertGreater(len(chunks), 1, "Should split into multiple word groups")
        
    def test_split_by_natural_breaks_characters(self):
        """Test splitting very long content without natural breaks."""
        # Very long content without spaces or breaks
        content = "x" * 10000  # 10k character string
        chunks = chunk_md.split_by_natural_breaks(content, 1000)
        
        # Should split into chunks with max size around 5000
        for chunk in chunks:
            self.assertLessEqual(len(chunk), 5000, "Chunks should not exceed max_chunk_size (5000)")
            
        # Should have broken content into multiple chunks
        self.assertGreater(len(chunks), 1, "Should split long content into multiple chunks")
        
    def test_group_elements_by_size_normal(self):
        """Test grouping elements when all are below target size."""
        elements = ["Short element 1", "Short element 2", "Short element 3"]
        chunks = chunk_md.group_elements_by_size(elements, 50, " ")
        
        # May be one or two chunks depending on exact implementation
        # Just verify we can group elements together
        self.assertLessEqual(len(chunks), 2, "Should group elements efficiently")
        
        # Make sure all elements are included
        combined = " ".join(chunks)
        self.assertIn("Short element 1", combined)
        self.assertIn("Short element 2", combined)
        self.assertIn("Short element 3", combined)
        
    def test_group_elements_by_size_large(self):
        """Test grouping elements when some exceed target size."""
        elements = ["Short element.", "This is a longer element that exceeds the target size.", "Another short one."]
        chunks = chunk_md.group_elements_by_size(elements, 20, " ")
        
        # Should create multiple chunks
        self.assertGreater(len(chunks), 1, "Should create multiple chunks")
        
    def test_group_elements_very_large(self):
        """Test grouping elements when one element is extremely large (>5000 chars)."""
        elements = ["Short element.", "x" * 8000, "Another short one."]
        chunks = chunk_md.group_elements_by_size(elements, 1000, " ")
        
        # The large element should be split into smaller chunks
        self.assertGreaterEqual(len(chunks), 3, "Very large element should be split")
        
        # No chunk should exceed max_chunk_size (around 5000)
        for chunk in chunks:
            self.assertLessEqual(len(chunk), 5100, "No chunk should exceed max_chunk_size (5000) by much")
        
    def test_long_string_with_natural_breaks(self):
        """Test splitting a long string that has natural break points."""
        # Long string with commas and spaces
        content = "This, is, a, very, long, string, with, commas, as, natural, break, points, " * 100
        chunks = chunk_md.split_by_natural_breaks(content, 200)
        
        # Should split at natural break points (commas)
        for chunk in chunks:
            # No chunk should significantly exceed the target size
            self.assertLess(len(chunk), 500, "Chunks should be split at natural break points")


if __name__ == "__main__":
    unittest.main()