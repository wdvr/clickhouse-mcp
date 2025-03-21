"""
Test the creation of a FAISS index from document chunks.

This test verifies that the FAISS index creation works correctly
with AWS Bedrock embeddings. It creates a small test index with
a few document chunks and verifies that the index can be loaded
and searched.

Requirements:
- AWS credentials with Bedrock access (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)
- Required packages: langchain-aws, faiss-cpu, langchain
"""

import os
import sys
import unittest
import shutil
from pathlib import Path
from unittest import skipUnless

# Add the parent directory to sys.path to allow importing the module
sys.path.append(str(Path(__file__).parent.parent))
from src.clickhouse_mcp.docs_search import load_chunks
from src.clickhouse_mcp.vector_search import create_faiss_index, load_faiss_index, vector_search
from src.clickhouse_mcp import DEFAULT_BEDROCK_MODEL, DEFAULT_REGION

class TestFaissIndex(unittest.TestCase):
    
    @skipUnless(os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"),
                reason="Skipping test as AWS credentials are not set in environment variables")
    def test_create_faiss_index(self):
        """Test creating a FAISS index with a very limited number of chunks."""
        try:
            from langchain_aws import BedrockEmbeddings
        except ImportError:
            self.skipTest("Required packages not installed: langchain_aws, faiss-cpu, langchain")
            
        # Load a few chunks
        chunks = load_chunks()
        test_chunks = chunks[:3]  # Use just 3 chunks for faster testing
        
        # Create a temporary directory for the index
        test_index_path = "/tmp/test_faiss_index"
        if os.path.exists(test_index_path):
            shutil.rmtree(test_index_path)
        
        # Initialize Bedrock Embeddings
        embeddings = BedrockEmbeddings(
            region_name=DEFAULT_REGION,
            model_id=DEFAULT_BEDROCK_MODEL
        )
        
        # Create the index
        create_faiss_index(
            chunks=test_chunks,
            output_path=test_index_path,
            embeddings=embeddings
        )
        
        # Check that the index was created
        self.assertTrue(os.path.exists(test_index_path))
        self.assertTrue(os.path.exists(os.path.join(test_index_path, "index.faiss")))
        self.assertTrue(os.path.exists(os.path.join(test_index_path, "index.pkl")))
        
        # Test loading and searching the index
        vector_store = load_faiss_index(test_index_path, embeddings)
        results = vector_search(vector_store, "test query", 1)
        
        # Check that we got a result
        self.assertEqual(len(results), 1)
        self.assertTrue(hasattr(results[0], 'page_content'))
        self.assertTrue(hasattr(results[0], 'metadata'))
        
        # Clean up
        shutil.rmtree(test_index_path)


if __name__ == "__main__":
    unittest.main()