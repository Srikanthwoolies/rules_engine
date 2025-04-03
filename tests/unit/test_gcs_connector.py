"""
Unit tests for the GCS connector.
Tests the GCSConnector class functionality.
"""

import pytest
from unittest.mock import MagicMock, patch
from rules_engine.connectors.gcs_connector import GCSConnector

@patch('rules_engine.connectors.gcs_connector.storage.Client')
def test_read_file(mock_client):
    """Test reading a file from GCS."""
    # Set up mocks
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_blob.download_as_text.return_value = "test content"
    mock_bucket.blob.return_value = mock_blob
    mock_client.return_value.get_bucket.return_value = mock_bucket
    
    # Create connector and read file
    connector = GCSConnector()
    content = connector.read_file("test-bucket", "test-file.csv")
    
    # Assert bucket and blob were accessed correctly
    mock_client.return_value.get_bucket.assert_called_once_with("test-bucket")
    mock_bucket.blob.assert_called_once_with("test-file.csv")
    mock_blob.download_as_text.assert_called_once()
    
    # Assert content was returned
    assert content == "test content"

@patch('rules_engine.connectors.gcs_connector.storage.Client')
def test_write_file(mock_client):
    """Test writing a file to GCS."""
    # Set up mocks
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client.return_value.get_bucket.return_value = mock_bucket
    
    # Create connector and write file
    connector = GCSConnector()
    connector.write_file("test-bucket", "test-file.txt", "test content")
    
    # Assert bucket and blob were accessed correctly
    mock_client.return_value.get_bucket.assert_called_once_with("test-bucket")
    mock_bucket.blob.assert_called_once_with("test-file.txt")
    mock_blob.upload_from_string.assert_called_once_with("test content")
