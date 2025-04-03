"""
Connector for Google Cloud Storage (GCS).
Handles reading files from and writing files to GCS buckets.
"""

from google.cloud import storage
import logging

logger = logging.getLogger("rules_engine")

class GCSConnector:
    """Connector for Google Cloud Storage operations."""
    
    def __init__(self):
        """Initialize the GCS connector."""
        self.client = storage.Client()
    
    def read_file(self, bucket_name, file_name):
        """
        Read a file from GCS.
        
        Args:
            bucket_name (str): Name of the GCS bucket
            file_name (str): Path to the file within the bucket
            
        Returns:
            str: The contents of the file as a string
        """
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(file_name)
        
        content = blob.download_as_text()
        return content
    
    def write_file(self, bucket_name, file_name, content):
        """
        Write content to a file in GCS.
        
        Args:
            bucket_name (str): Name of the GCS bucket
            file_name (str): Path to the file within the bucket
            content (str): Content to write to the file
            
        Returns:
            None
        """
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(file_name)
        
        blob.upload_from_string(content)
        logger.info(f"Wrote content to gs://{bucket_name}/{file_name}")
