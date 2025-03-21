#!/usr/bin/env python3

import sys
import os
import unittest
from pathlib import Path

# Add the tools directory to the path so we can import chunk_md
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tools"))
import chunk_md


class TestPolygonChunk(unittest.TestCase):
    """Test the polygon chunking specifically."""
    
    def test_long_polygon_definition(self):
        """Test chunking of extremely long polygon definitions."""
        # Create a long polygon definition similar to what was in the problematic file
        polygon_points = []
        for i in range(300):  # Create 300 points
            x = i * 0.1
            y = i * 0.2
            polygon_points.append(f"({x},{y})")
        
        polygon_definition = "POLYGON((" + ",".join(polygon_points) + "))"
        
        # Add some context around the polygon
        content = f"""# Polygon Function

## Example Usage

Here is an example of a large polygon:

```sql
SELECT polygonArea({polygon_definition});
```

The function calculates the area of the polygon.
"""
        
        # Chunk the content with a small target size to force splitting
        chunks = chunk_md.split_by_natural_breaks(content, 1000)
        
        # Verify we have chunks
        self.assertGreater(len(chunks), 1, "Long polygon should be split into multiple chunks")
        
        # Ensure no chunk exceeds the maximum size
        for chunk in chunks:
            self.assertLessEqual(len(chunk), 5000, "No chunk should exceed max_chunk_size (5000)")
            
    def test_extremely_long_single_line(self):
        """Test chunking of an extremely long single line with no breaks."""
        # Create an extremely long line (12k characters)
        long_line = "x" * 12000
        
        content = f"""# Long Line Test

## Example

{long_line}

Some text after the long line.
"""
        
        # Chunk the content
        chunks = chunk_md.split_by_natural_breaks(content, 5000)
        
        # Verify we have multiple chunks
        self.assertGreater(len(chunks), 1, "Long line should be split into multiple chunks")
        
        # Ensure no chunk exceeds the maximum size
        for chunk in chunks:
            self.assertLessEqual(len(chunk), 5500, "No chunk should exceed max_chunk_size (5000) significantly")
            
    def test_long_json_object(self):
        """Test chunking of a long JSON object."""
        # Create a long JSON object
        json_items = []
        for i in range(500):
            json_items.append(f'"key{i}": "value{i}"')
        
        json_object = "{\n" + ",\n".join(json_items) + "\n}"
        
        content = f"""# JSON Example

## Large JSON Object

```json
{json_object}
```
"""
        
        # Chunk the content
        chunks = chunk_md.split_by_natural_breaks(content, 1000)
        
        # Verify we have multiple chunks
        self.assertGreater(len(chunks), 1, "Long JSON should be split into multiple chunks")
        
        # Ensure no chunk exceeds the maximum size
        for chunk in chunks:
            self.assertLessEqual(len(chunk), 5000, "No chunk should exceed max_chunk_size (5000)")


if __name__ == "__main__":
    unittest.main()