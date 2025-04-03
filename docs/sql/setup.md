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
