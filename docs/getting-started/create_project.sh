#!/bin/bash

# Set project name
PROJECT_NAME="gcs_rules_engine"
PACKAGE_NAME="rules_engine"

# Create project directory structure
mkdir -p $PROJECT_NAME/{docs,src/$PACKAGE_NAME/{config,connectors,models,utils,subpackage},tests/{unit,integration}}
mkdir -p $PROJECT_NAME/src/$PACKAGE_NAME/subpackage

# Create root files
cat > $PROJECT_NAME/.gitignore << 'EOL'
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
*.egg-info/
.installed.cfg
*.egg

# Virtual environments
venv/
ENV/
env/

# IDE
.idea/
.vscode/
*.swp
*.swo

# GCP
.gcp-service-account.json
credentials.json

# Local development
.env
.env.local
EOL

cat > $PROJECT_NAME/README.md << 'EOL'
# GCS Rules Engine

A Cloud Function that processes data from Google Cloud Storage (GCS), applies rules stored in BigQuery, and logs rule violations.

## Overview

This project implements a serverless rules engine that:
1. Triggers on GCS bucket events (file uploads/updates)
2. Reads data from the uploaded file 
3. Fetches rules definitions from BigQuery
4. Applies the rules to the data
5. Logs any rule violations back to BigQuery

## Architecture

```
┌───────────┐     ┌───────────────┐     ┌───────────────┐
│  GCS File │────▶│ Cloud Function │────▶│ BigQuery Rule │
│  Upload   │     │ (Rules Engine) │◀────│ Definitions   │
└───────────┘     └───────┬───────┘     └───────────────┘
                          │
                          ▼
                  ┌───────────────┐
                  │ BigQuery      │
                  │ Rule          │
                  │ Violations Log│
                  └───────────────┘
```

## Setup Instructions

### Prerequisites
- Google Cloud Platform (GCP) account
- gcloud CLI installed
- Python 3.8+

### Installation

1. Clone this repository:
   ```
   git clone https://github.com/your-username/gcs_rules_engine.git
   cd gcs_rules_engine
   ```

2. Set up a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. Set up GCP resources:
   ```
   # Set your GCP project
   gcloud config set project YOUR_PROJECT_ID
   
   # Create a GCS bucket (if needed)
   gsutil mb gs://YOUR_BUCKET_NAME/
   
   # Create BigQuery dataset (if needed)
   bq mk rules_engine
   ```

4. Deploy the Cloud Function:
   ```
   gcloud functions deploy process_gcs_file \
     --runtime python38 \
     --trigger-resource YOUR_BUCKET_NAME \
     --trigger-event google.storage.object.finalize \
     --entry-point process_gcs_file \
     --source src/
   ```

### Configuration

1. Create the BigQuery tables for rule definitions and violations (see `docs/sql/setup.md`)
2. Add your rules to the `rules_definition` table
3. Upload a file to the GCS bucket to trigger the rules engine

## Usage

1. Upload a file to your configured GCS bucket
2. The Cloud Function will automatically process the file and apply rules
3. Check the BigQuery `rule_violations` table for any detected violations

## Development

See the [Development Guide](docs/development.md) for more information.
EOL

# Create requirements.txt
cat > $PROJECT_NAME/requirements.txt << 'EOL'
# Google Cloud dependencies
google-cloud-storage>=2.0.0
google-cloud-bigquery>=2.34.0
functions-framework>=3.0.0

# Data processing
pandas>=1.3.0
numpy>=1.21.0

# Testing and linting
pytest>=6.2.5
pytest-cov>=2.12.1
flake8>=3.9.2
black>=21.6b0
EOL

# Create main package files
cat > $PROJECT_NAME/src/$PACKAGE_NAME/__init__.py << 'EOL'
"""Rules Engine package for processing GCS files and applying BigQuery rules."""

__version__ = "0.1.0"
EOL

# Create main module with Cloud Function entry point
cat > $PROJECT_NAME/src/$PACKAGE_NAME/main.py << 'EOL'
"""
Cloud Function entry point for the GCS Rules Engine.
This module contains the main Cloud Function that is triggered by GCS events.
"""

import json
import functions_framework
from google.cloud import storage, bigquery
import logging
import pandas as pd
import io

from rules_engine.connectors.bigquery_connector import BigQueryConnector
from rules_engine.connectors.gcs_connector import GCSConnector
from rules_engine.models.rule import Rule
from rules_engine.utils.logger import setup_logger

# Set up logging
logger = setup_logger("rules_engine")

@functions_framework.cloud_event
def process_gcs_file(cloud_event):
    """
    Cloud Function triggered by a finalize event on a GCS bucket.
    
    Args:
        cloud_event (CloudEvent): The CloudEvent that triggered the function
        
    Returns:
        None: The function logs its progress and writes to BigQuery
    """
    logger.info("Starting rules engine processing")
    
    try:
        # Extract GCS file information from the event
        payload = cloud_event.data
        bucket_name = payload["bucket"]
        file_name = payload["name"]
        
        logger.info(f"Processing file: gs://{bucket_name}/{file_name}")
        
        # Initialize connectors
        gcs = GCSConnector()
        bq = BigQueryConnector()
        
        # Read file from GCS
        file_data = gcs.read_file(bucket_name, file_name)
        
        # Parse file data (assuming CSV for this example)
        df = pd.read_csv(io.StringIO(file_data))
        logger.info(f"Loaded data with {len(df)} rows")
        
        # Fetch rules from BigQuery
        rules = bq.get_rules()
        logger.info(f"Fetched {len(rules)} rules from BigQuery")
        
        # Apply each rule to the data
        violations = []
        for rule in rules:
            rule_violations = rule.apply(df)
            if rule_violations:
                violations.extend(rule_violations)
                logger.warning(f"Rule {rule.rule_number} found {len(rule_violations)} violations")
        
        # Log violations to BigQuery if any were found
        if violations:
            bq.log_violations(violations)
            logger.info(f"Logged {len(violations)} total violations to BigQuery")
        else:
            logger.info("No violations found")
            
        return f"Processed {file_name} successfully"
        
    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        raise
EOL

# Create connectors subpackage
cat > $PROJECT_NAME/src/$PACKAGE_NAME/connectors/__init__.py << 'EOL'
"""Connectors for external data sources and sinks."""
EOL

cat > $PROJECT_NAME/src/$PACKAGE_NAME/connectors/gcs_connector.py << 'EOL'
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
EOL

cat > $PROJECT_NAME/src/$PACKAGE_NAME/connectors/bigquery_connector.py << 'EOL'
"""
Connector for BigQuery.
Handles reading from and writing to BigQuery tables, including fetching rules
and logging rule violations.
"""

from google.cloud import bigquery
import logging
import uuid
from datetime import datetime

from rules_engine.models.rule import Rule

logger = logging.getLogger("rules_engine")

class BigQueryConnector:
    """Connector for BigQuery operations."""
    
    def __init__(self, project_id=None, dataset_id="rules_engine"):
        """
        Initialize the BigQuery connector.
        
        Args:
            project_id (str, optional): GCP project ID. If None, default project will be used.
            dataset_id (str, optional): BigQuery dataset ID. Defaults to "rules_engine".
        """
        self.client = bigquery.Client(project=project_id)
        self.dataset_id = dataset_id
        self.rules_table = "rules_definition"
        self.violations_table = "rule_violations"
    
    def get_rules(self):
        """
        Fetch rules from the BigQuery rules table.
        
        Returns:
            list: List of Rule objects
        """
        query = f"""
        SELECT 
            rule_number, 
            rule_description, 
            rule_query
        FROM `{self.dataset_id}.{self.rules_table}`
        """
        
        query_job = self.client.query(query)
        rows = query_job.result()
        
        rules = []
        for row in rows:
            rule = Rule(
                rule_number=row.rule_number,
                rule_description=row.rule_description,
                rule_query=row.rule_query
            )
            rules.append(rule)
        
        return rules
    
    def log_violations(self, violations):
        """
        Log rule violations to the BigQuery violations table.
        
        Args:
            violations (list): List of violation dictionaries
            
        Returns:
            None
        """
        if not violations:
            return
        
        rows_to_insert = []
        
        for violation in violations:
            row = {
                "violation_id": str(uuid.uuid4()),
                "rule_number": violation["rule_number"],
                "rule_description": violation["rule_description"],
                "violation_timestamp": datetime.now().isoformat(),
                "violation_details": violation["details"]
            }
            rows_to_insert.append(row)
        
        errors = self.client.insert_rows_json(
            f"{self.dataset_id}.{self.violations_table}", 
            rows_to_insert
        )
        
        if errors:
            logger.error(f"Errors inserting rows: {errors}")
        else:
            logger.info(f"Inserted {len(rows_to_insert)} violation records")
EOL

# Create models subpackage
cat > $PROJECT_NAME/src/$PACKAGE_NAME/models/__init__.py << 'EOL'
"""Models used by the rules engine."""
EOL

cat > $PROJECT_NAME/src/$PACKAGE_NAME/models/rule.py << 'EOL'
"""
Rule model.
Represents a business rule that can be applied to data.
"""

import json
import pandas as pd

class Rule:
    """Rule class representing a business rule that can be applied to data."""
    
    def __init__(self, rule_number, rule_description, rule_query):
        """
        Initialize a rule.
        
        Args:
            rule_number (str): Unique identifier for the rule
            rule_description (str): Human-readable description of what the rule checks
            rule_query (str): SQL-like query that identifies violations
        """
        self.rule_number = rule_number
        self.rule_description = rule_description
        self.rule_query = rule_query
    
    def apply(self, dataframe):
        """
        Apply the rule to a dataframe.
        
        This is a simplified implementation that assumes the rule_query can be
        translated to pandas operations. In a real implementation, this would 
        likely use a more sophisticated query engine.
        
        Args:
            dataframe (pd.DataFrame): Data to check for rule violations
            
        Returns:
            list: List of violation dictionaries
        """
        # For this example, we'll assume a simple rule_query format
        # In practice, this would need to parse and execute the SQL-like query
        
        # Simplified example: assume rule_query is a condition that can be evaluated
        # Example: "amount < 0" or "status == 'ERROR'"
        try:
            # Apply the rule condition
            # NOTE: This is a simplified implementation for demonstration purposes
            # In production, you would need a more robust way to parse and apply SQL queries
            violations_df = dataframe.query(self.rule_query)
            
            if len(violations_df) == 0:
                return []
            
            # Create violation records
            violations = []
            for _, row in violations_df.iterrows():
                violation = {
                    "rule_number": self.rule_number,
                    "rule_description": self.rule_description,
                    "details": row.to_json()
                }
                violations.append(violation)
            
            return violations
            
        except Exception as e:
            # In a real system, we'd handle this more gracefully
            print(f"Error applying rule {self.rule_number}: {str(e)}")
            return []
    
    def __repr__(self):
        """String representation of the rule."""
        return f"Rule({self.rule_number}: {self.rule_description})"
EOL

# Create utils subpackage
cat > $PROJECT_NAME/src/$PACKAGE_NAME/utils/__init__.py << 'EOL'
"""Utility functions and classes for the rules engine."""
EOL

cat > $PROJECT_NAME/src/$PACKAGE_NAME/utils/logger.py << 'EOL'
"""
Logger setup and configuration.
Provides consistent logging across the application.
"""

import logging

def setup_logger(name, log_level=logging.INFO):
    """
    Set up and configure a logger.
    
    Args:
        name (str): Name of the logger
        log_level (int, optional): Logging level. Defaults to logging.INFO.
        
    Returns:
        Logger: Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # Create console handler if no handlers exist
    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(console_handler)
    
    return logger
EOL

# Create a basic subpackage
cat > $PROJECT_NAME/src/$PACKAGE_NAME/subpackage/__init__.py << 'EOL'
"""Subpackage for additional functionality."""
EOL

cat > $PROJECT_NAME/src/$PACKAGE_NAME/subpackage/another_module.py << 'EOL'
"""
Example subpackage module.
This module demonstrates how to organize code in subpackages.
"""

def example_function():
    """Example function that does nothing."""
    return "This is an example function"
EOL

# Create test files
cat > $PROJECT_NAME/tests/__init__.py << 'EOL'
"""Test package for the rules engine."""
EOL

cat > $PROJECT_NAME/tests/unit/__init__.py << 'EOL'
"""Unit tests for the rules engine."""
EOL

cat > $PROJECT_NAME/tests/unit/test_rule.py << 'EOL'
"""
Unit tests for the Rule model.
Tests the Rule class functionality.
"""

import pytest
import pandas as pd
from rules_engine.models.rule import Rule

def test_rule_initialization():
    """Test that a Rule can be initialized with the correct properties."""
    rule = Rule(
        rule_number="R001",
        rule_description="Test rule",
        rule_query="amount < 0"
    )
    
    assert rule.rule_number == "R001"
    assert rule.rule_description == "Test rule"
    assert rule.rule_query == "amount < 0"

def test_apply_rule_with_violations():
    """Test that applying a rule properly identifies violations."""
    # Create test data with violations
    data = {
        "id": [1, 2, 3, 4],
        "amount": [100, -50, 200, -10]
    }
    df = pd.DataFrame(data)
    
    # Create a rule to check for negative amounts
    rule = Rule(
        rule_number="R001",
        rule_description="Amount must be positive",
        rule_query="amount < 0"
    )
    
    violations = rule.apply(df)
    
    # Should find 2 violations
    assert len(violations) == 2
    
    # Check violation details
    assert violations[0]["rule_number"] == "R001"
    assert violations[0]["rule_description"] == "Amount must be positive"
    
    # The violation should contain the row data
    import json
    violation_data = json.loads(violations[0]["details"])
    assert violation_data["amount"] == -50

def test_apply_rule_without_violations():
    """Test that applying a rule to compliant data returns no violations."""
    # Create test data without violations
    data = {
        "id": [1, 2, 3],
        "amount": [100, 50, 200]
    }
    df = pd.DataFrame(data)
    
    # Create a rule to check for negative amounts
    rule = Rule(
        rule_number="R001",
        rule_description="Amount must be positive",
        rule_query="amount < 0"
    )
    
    violations = rule.apply(df)
    
    # Should find no violations
    assert len(violations) == 0
EOL

cat > $PROJECT_NAME/tests/unit/test_gcs_connector.py << 'EOL'
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
EOL

# Create documentation files
cat > $PROJECT_NAME/docs/development.md << 'EOL'
# Development Guide

This document provides information for developers working on the GCS Rules Engine project.

## Project Structure

```
gcs_rules_engine/
├── docs/               # Documentation
├── src/                # Source code
│   └── rules_engine/   # Main package
│       ├── connectors/ # External system connectors (GCS, BigQuery)
│       ├── models/     # Data models and business logic
│       ├── utils/      # Utility functions and helpers
│       └── subpackage/ # Additional functionality
└── tests/              # Tests
    ├── unit/           # Unit tests
    └── integration/    # Integration tests
```

## Development Setup

1. Clone the repository
2. Set up a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```
3. Install development dependencies:
   ```
   pip install -e .  # Install in development mode
   ```

## Running Tests

Run the test suite with pytest:

```
pytest
```

For coverage report:

```
pytest --cov=rules_engine
```

## Local Development

For local development, you can use the Functions Framework to test your function:

```
functions-framework --target=process_gcs_file --signature-type=event
```

Then, send a test request:

```
curl localhost:8080 \
  -H "Content-Type: application/json" \
  -d '{
    "bucket": "test-bucket",
    "name": "test-file.csv"
  }'
```

## Coding Standards

This project follows PEP 8 style guidelines. Code formatting is done with Black:

```
black src/ tests/
```

Linting is done with flake8:

```
flake8 src/ tests/
```

## CI/CD Pipeline

The CI/CD pipeline runs on every push to the repository and:

1. Runs the test suite
2. Checks code formatting and linting
3. Deploys the Cloud Function (only on main branch)

For more details, see the GitHub Actions workflow files.

## Adding New Rules

Rules are stored in BigQuery in the `rules_definition` table. Each rule consists of:

- `rule_number`: A unique identifier for the rule
- `rule_description`: A human-readable description of what the rule checks
- `rule_query`: A query that identifies violations of the rule

To add a new rule, insert a row into the `rules_definition` table.
EOL

cat > $PROJECT_NAME/docs/sql/setup.md << 'EOL'
# BigQuery Setup

This document provides SQL scripts to set up the required BigQuery tables for the rules engine.

## Create Rules Definition Table

The `rules_definition` table stores the rules that will be applied to data.

```sql
CREATE TABLE IF NOT EXISTS `rules_engine.rules_definition` (
  rule_number STRING,
  rule_description STRING,
  rule_query STRING
);
```

## Create Rule Violations Table

The `rule_violations` table stores detected violations of the rules.

```sql
CREATE TABLE IF NOT EXISTS `rules_engine.rule_violations` (
  violation_id STRING,
  rule_number STRING,
  rule_description STRING,
  violation_timestamp TIMESTAMP,
  violation_details STRING
);
```

## Sample Rules

Here are some example rules to get started:

```sql
-- Insert example rules
INSERT INTO `rules_engine.rules_definition` (rule_number, rule_description, rule_query)
VALUES 
  ('R001', 'Amount must be positive', 'amount < 0'),
  ('R002', 'Status must be valid', 'status NOT IN ("APPROVED", "PENDING", "REJECTED")'),
  ('R003', 'Customer ID must be provided', 'customer_id IS NULL OR customer_id = ""');
```

## Sample Query to Check Violations

Use this query to check for rule violations:

```sql
SELECT 
  rule_number, 
  rule_description,
  COUNT(*) as violation_count
FROM `rules_engine.rule_violations`
WHERE DATE(violation_timestamp) = CURRENT_DATE()
GROUP BY rule_number, rule_description
ORDER BY rule_number;
```
EOL

# Make the script executable
chmod +x create_project.sh

echo "Project structure script created successfully!" 