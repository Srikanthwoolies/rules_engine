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
