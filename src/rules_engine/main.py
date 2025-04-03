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
