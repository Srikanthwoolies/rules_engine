-- Rules Engine BigQuery Procedural Script
-- This script creates a rules engine that reads rules from a Google Sheet,
-- executes the rules, and logs violations to a table.

DECLARE project_id STRING DEFAULT 'your-project-id'; -- Replace with your actual project ID
DECLARE dataset_id STRING DEFAULT 'rules_engine'; -- Replace with your desired dataset ID
DECLARE rules_sheet_url STRING DEFAULT 'https://docs.google.com/spreadsheets/d/your-sheet-id/edit'; -- Replace with your Google Sheet URL

-- Set the full dataset path
DECLARE dataset_path STRING DEFAULT CONCAT(project_id, '.', dataset_id);

-- Create the dataset if it doesn't exist
EXECUTE IMMEDIATE FORMAT("""
  CREATE SCHEMA IF NOT EXISTS `%s`
""", dataset_path);

-- Create an external table that references the Google Sheet containing rules
EXECUTE IMMEDIATE FORMAT("""
  CREATE OR REPLACE EXTERNAL TABLE `%s.rules_definition`
  OPTIONS (
    format = 'GOOGLE_SHEETS',
    uris = ['%s'],
    skip_leading_rows = 1
  ) AS
  SELECT
    CAST(rule_number AS STRING) AS rule_number,
    CAST(rule_description AS STRING) AS rule_description,
    CAST(rule_query AS STRING) AS rule_query
  FROM (
    SELECT 
      'rule_number' AS rule_number,
      'rule_description' AS rule_description,
      'rule_query' AS rule_query
    WHERE FALSE
  )
""", dataset_path, rules_sheet_url);

-- Create a table for storing rule violations if it doesn't exist
EXECUTE IMMEDIATE FORMAT("""
  CREATE TABLE IF NOT EXISTS `%s.rule_violations` (
    violation_id STRING,
    rule_number STRING,
    rule_description STRING,
    violation_timestamp TIMESTAMP,
    violation_details STRING
  )
""", dataset_path);

-- Procedure to run all rules and log violations
BEGIN
  -- Create a temporary table to hold the rules from the Google Sheet
  CREATE TEMP TABLE temp_rules AS
  SELECT 
    rule_number,
    rule_description,
    rule_query
  FROM `%s.rules_definition`;
  
  -- Loop through each rule and execute it
  FOR rule_record IN (SELECT * FROM temp_rules)
  DO
    DECLARE violation_count INT64;
    DECLARE result_query STRING;
    
    -- Construct the query to execute the rule and count violations
    SET result_query = FORMAT("""
      WITH rule_results AS (
        %s
      )
      SELECT COUNT(*) AS violation_count FROM rule_results
    """, rule_record.rule_query);
    
    -- Execute the rule query and get the violation count
    EXECUTE IMMEDIATE result_query INTO violation_count;
    
    -- If violations exist, execute the rule again to get details and log them
    IF violation_count > 0 THEN
      DECLARE details_cursor ARRAY<STRUCT<details STRING>>;
      DECLARE insertion_query STRING;
      
      -- Get the detailed violations
      EXECUTE IMMEDIATE FORMAT("""
        WITH rule_results AS (
          %s
        )
        SELECT TO_JSON(t) AS details FROM rule_results t
      """, rule_record.rule_query) INTO details_cursor;
      
      -- Log each violation to the violations table
      FOR detail IN (SELECT details FROM UNNEST(details_cursor))
      DO
        EXECUTE IMMEDIATE FORMAT("""
          INSERT INTO `%s.rule_violations` (
            violation_id,
            rule_number,
            rule_description,
            violation_timestamp,
            violation_details
          )
          VALUES (
            GENERATE_UUID(),
            '%s',
            '%s',
            CURRENT_TIMESTAMP(),
            '%s'
          )
        """, dataset_path, rule_record.rule_number, rule_record.rule_description, detail.details);
      END FOR;
    END IF;
  END FOR;
END;

-- Output a summary of violations
EXECUTE IMMEDIATE FORMAT("""
  SELECT
    rule_number,
    rule_description,
    COUNT(*) AS violation_count
  FROM `%s.rule_violations`
  WHERE DATE(violation_timestamp) = CURRENT_DATE()
  GROUP BY rule_number, rule_description
  ORDER BY rule_number
""", dataset_path); 