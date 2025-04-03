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
