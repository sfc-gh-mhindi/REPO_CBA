-- ============================================================================
-- P_SET_CHUNKING_KEY - Configure Chunking Method for Migration Tables
-- Description: Sets the checksum method and chunking configuration for specific tables
-- Created: 2025
-- ============================================================================


-- update migration_tracking.dmva_object_info
-- set checksum_method = object_construct('column_name', 'CODE1', 'type', 'by_integer', 'modulus', '1000000')
-- where system_name = 'teradata_source'
-- and schema_name = 'B_D52_D_TMP_001_STD_0'
-- and object_name = ,'SANDPIT_DEMO_TBL001';

CREATE OR REPLACE PROCEDURE npd_d12_dmn_gdwmig.migration_tracking.P_SET_CHUNKING_KEY(
    source_database_name STRING,
    source_table_name STRING,
    column_name STRING,
    chunking_value STRING,
    data_type STRING  -- Valid values: 'by_integer', 'by_date', 'by_substr'
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    result_message STRING DEFAULT '';
    rows_affected INTEGER DEFAULT 0;
    json_property_name STRING DEFAULT '';
BEGIN
    
    result_message := 'Setting chunking key configuration...\n';
    result_message := result_message || 'Source: ' || source_database_name || '.' || source_table_name || '\n';
    result_message := result_message || 'Column: ' || column_name || '\n';
    result_message := result_message || 'Chunking Value: ' || chunking_value || '\n';
    result_message := result_message || 'Data Type: ' || data_type || '\n';
    
    -- Validate data_type and set appropriate JSON property name
    IF (data_type = 'by_integer') THEN
        json_property_name := 'modulus';
    ELSEIF (data_type = 'by_date') THEN
        json_property_name := 'period';
    ELSEIF (data_type = 'by_substr') THEN
        json_property_name := 'length';
    ELSE
        result_message := result_message || 'ERROR: Invalid data_type value: ' || data_type || '\n';
        result_message := result_message || 'Valid values are: by_integer, by_date, by_substr\n';
        RETURN result_message;
    END IF;
    
    result_message := result_message || 'JSON Property: ' || json_property_name || '\n';
    
    -- Update the checksum method for the specified table
    UPDATE migration_tracking.dmva_object_info
    SET checksum_method = object_construct(
        'column_name', :column_name, 
        'type', :data_type, 
        :json_property_name, :chunking_value
    )
    WHERE system_name = 'teradata_source'
    AND schema_name = :source_database_name
    AND object_name = :source_table_name;
    
    -- Get the number of rows affected
    rows_affected := SQLROWCOUNT;
    
    result_message := result_message || 'Rows updated: ' || rows_affected || '\n';
    
    IF (rows_affected = 0) THEN
        result_message := result_message || 'WARNING: No rows were updated. Please verify:\n';
        result_message := result_message || '- Table exists in dmva_object_info\n';
        result_message := result_message || '- system_name = ''teradata_source''\n';
        result_message := result_message || '- schema_name = ''' || source_database_name || '''\n';
        result_message := result_message || '- object_name = ''' || source_table_name || '''\n';
    ELSE
        result_message := result_message || 'Chunking key configuration updated successfully!\n';
    END IF;
    
    RETURN result_message;
    
    EXCEPTION
    WHEN OTHER THEN
        result_message := result_message || 'ERROR: ' || SQLERRM || '\n';
        RETURN result_message;
END;
$$;

-- ============================================================================
-- EXECUTION INSTRUCTIONS
-- ============================================================================
/*
To execute this chunking key configuration procedure, run:

CALL P_SET_CHUNKING_KEY(
    'B_D52_D_TMP_001_STD_0',    -- source_database_name (Teradata schema)
    'SANDPIT_DEMO_TBL001',      -- source_table_name
    'CODE1',                    -- column_name (chunking column)
    '1000000',                  -- chunking_value (modulus/period/length value)
    'by_integer'                -- data_type (MUST be: by_integer, by_date, or by_substr)
);

Examples for different chunking types:

-- Integer chunking (uses 'modulus' property)
CALL P_SET_CHUNKING_KEY(
    'MY_SCHEMA',
    'MY_TABLE',
    'ID_COLUMN',
    '500000',
    'by_integer'
);

-- Date-based chunking (uses 'period' property)
CALL P_SET_CHUNKING_KEY(
    'MY_SCHEMA',
    'MY_TABLE',
    'DATE_COLUMN',
    '100',
    'by_date'
);

-- String substring chunking (uses 'length' property)
CALL P_SET_CHUNKING_KEY(
    'MY_SCHEMA',
    'MY_TABLE',
    'STRING_COLUMN',
    '10',
    'by_substr'
);

Data Type Validation:
- data_type MUST be one of: 'by_integer', 'by_date', 'by_substr'
- Any other value will return an error message

JSON Property Mapping:
- by_integer → uses 'modulus' property in checksum_method JSON
- by_date → uses 'period' property in checksum_method JSON  
- by_substr → uses 'length' property in checksum_method JSON

This procedure:
1. Validates the data_type parameter (by_integer, by_date, by_substr only)
2. Maps data_type to the correct JSON property name
3. Updates the checksum_method in dmva_object_info table
4. Provides feedback on the number of rows affected
5. Includes error handling and validation warnings
6. Returns detailed status messages

Note: This should be run BEFORE starting the migration procedure if custom chunking is required.
The chunking configuration helps optimize data transfer for large tables by splitting them into smaller chunks.
*/

-- ============================================================================
-- VERIFICATION QUERY
-- ============================================================================
/*
-- Verify the chunking configuration was applied correctly:
SELECT 
    system_name,
    schema_name,
    object_name,
    checksum_method
FROM migration_tracking.dmva_object_info
WHERE system_name = 'teradata_source'
AND schema_name = 'B_D52_D_TMP_001_STD_0'
AND object_name = 'SANDPIT_DEMO_TBL001';
*/
