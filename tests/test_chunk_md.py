#!/usr/bin/env python3

import sys
import os
import unittest
import random
from pathlib import Path

# Add the tools directory to the path so we can import chunk_md
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tools"))
import chunk_md

class TestChunkMarkdown(unittest.TestCase):

    def test_extract_frontmatter(self):
        """Test that frontmatter is correctly extracted."""
        # Content with frontmatter
        content = """---
title: Test Document
description: This is a test document
---

# Test Document

This is the content."""

        frontmatter, content_without_frontmatter = chunk_md.extract_frontmatter(content)
        
        # Check frontmatter
        self.assertEqual(frontmatter["title"], "Test Document")
        self.assertEqual(frontmatter["description"], "This is a test document")
        
        # Check content
        self.assertIn("# Test Document", content_without_frontmatter)
        self.assertIn("This is the content.", content_without_frontmatter)
        self.assertNotIn("---", content_without_frontmatter)
        
    def test_build_chunk_key(self):
        """Test that chunk keys are correctly built."""
        # Simple key
        key = chunk_md.build_chunk_key("base-path", ["Section"])
        self.assertEqual(key, "base-path::section")
        
        # Key with multiple sections
        key = chunk_md.build_chunk_key("base-path", ["Section 1", "Section 2"])
        self.assertEqual(key, "base-path::section-1-section-2")
        
        # Key with special characters
        key = chunk_md.build_chunk_key("base-path", ["Section's with (special) characters!"])
        self.assertEqual(key, "base-path::sections-with-special-characters")
        
        # Key with no sections
        key = chunk_md.build_chunk_key("base-path", [])
        self.assertEqual(key, "base-path")
        
    def test_get_section_title_from_content(self):
        """Test extracting section titles from content."""
        # Content with H2
        h2_content = "## Section Title\nThis is some content."
        self.assertEqual(chunk_md.get_section_title_from_content(h2_content), "Section Title")
        
        # Content with H3
        h3_content = "### Section Title\nThis is some content."
        self.assertEqual(chunk_md.get_section_title_from_content(h3_content), "Section Title")
        
        # Content with no header
        no_header_content = "This is just content with no header."
        self.assertEqual(chunk_md.get_section_title_from_content(no_header_content), "Section")
        
    def test_find_headers(self):
        """Test finding headers in content."""
        # Content with H2 headers
        content = """# Document Title

Introduction text.

## Section 1

Content for section 1.

## Section 2

Content for section 2.

## Section 3

Content for section 3."""

        headers = chunk_md.find_headers(content, 2)
        
        # Should find introduction and 3 sections
        self.assertEqual(len(headers), 4)
        self.assertEqual(headers[0]["title"], "Introduction")
        self.assertEqual(headers[1]["title"], "Section 1")
        self.assertEqual(headers[2]["title"], "Section 2")
        self.assertEqual(headers[3]["title"], "Section 3")
        
        # Check content
        self.assertIn("Introduction text.", headers[0]["content"])
        self.assertIn("Content for section 1.", headers[1]["content"])
        self.assertIn("Content for section 2.", headers[2]["content"])
        self.assertIn("Content for section 3.", headers[3]["content"])
        

if __name__ == "__main__":
    unittest.main()