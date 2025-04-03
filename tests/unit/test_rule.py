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
