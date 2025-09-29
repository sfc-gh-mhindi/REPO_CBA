-- =====================================================================
-- Snowflake Connection Setup (Converted from BTEQ)
-- Original: .logon %%GDW_HOST%%/%%GDW_USER%%,%%GDW_PASS%%;
-- =====================================================================

-- Snowflake connection is handled through session context
-- Variables should be set as session parameters or passed to stored procedures

-- Option 1: Set session parameters
-- USE WAREHOUSE <warehouse_name>;
-- USE DATABASE <database_name>; 
-- USE SCHEMA <schema_name>;

-- Option 2: Create a connection procedure
CREATE OR REPLACE PROCEDURE SETUP_CONNECTION(
    WAREHOUSE_NAME STRING,
    DATABASE_NAME STRING,
    SCHEMA_NAME STRING DEFAULT 'PUBLIC'
)
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Snowflake equivalent of BTEQ login - sets up session context'
AS
$$
BEGIN
    EXECUTE IMMEDIATE 'USE WAREHOUSE ' || WAREHOUSE_NAME;
    EXECUTE IMMEDIATE 'USE DATABASE ' || DATABASE_NAME;
    EXECUTE IMMEDIATE 'USE SCHEMA ' || SCHEMA_NAME;
    
    RETURN 'Connection setup completed successfully for database: ' || DATABASE_NAME;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Connection setup failed: ' || SQLERRM;
END;
$$;

-- Usage example:
-- CALL SETUP_CONNECTION('GDW_WAREHOUSE', 'GDW_DATABASE', 'GDW_SCHEMA'); 