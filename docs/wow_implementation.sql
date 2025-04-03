DECLARE result_query STRING;
-- Procedure to run all rules and log violations
BEGIN
  -- Loop through each rule and execute it
  FOR rule_record IN (select description, query
from `gcp-wow-ent-im-tbl-dev.adp_control_data.ext_gsheet_rules_definition`
where query is not null)
  DO
      
    -- Construct the query to execute the rule and count violations
    SET result_query = FORMAT("""
    
INSERT INTO `gcp-wow-ent-im-tbl-dev.adp_control_data.rules_engine_violations` 

with FRAMEWORK_BASE as (  
%s
)
SELECT 
      '%s' as alert, 
      cast(null as string) as keys, 
      TO_JSON_STRING(ARRAY_AGG(b))  as data,
      current_timestamp as insert_ts
FROM  FRAMEWORK_BASE b
/* Prevent null rows to be inserted due to ARRAY_AGG */
qualify any_value(data) over () !=  'null'
;
    """, rule_record.query, rule_record.description);
    
    -- Execute the rule query and get the violation count
    EXECUTE IMMEDIATE result_query;
    
  END FOR;
END;
 