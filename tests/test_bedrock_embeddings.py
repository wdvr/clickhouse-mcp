import os
import unittest
from unittest import skipUnless

from langchain_aws import BedrockEmbeddings

class TestBedrockEmbeddings(unittest.TestCase):

    @skipUnless(os.getenv("AWS_ACCESS_KEY_ID"),
                reason="Skipping real test as AWS credentials are not set in environment variables (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set) or the required permissions are not granted to the user.")
    def test_bedrock_embeddings_real(self):
        """This test requires proper AWS credentials with Bedrock access."""
        # Check if we should run the real test
        try:
            # Initialize Bedrock Embeddings
            embeddings = BedrockEmbeddings(
                region_name="us-east-1",
                model_id="amazon.titan-embed-text-v2:0"
            )
            
            # Generate embeddings for a single query
            query_text = "This is a sample query."
            query_embedding = embeddings.embed_query(query_text)

            # debug-print
            print(f"Query: {query_text}")
            print(f"Embedding: {query_embedding}")


        except Exception as e:
            print(f"Real test failed: {e}")
            raise

if __name__ == "__main__":
    unittest.main()