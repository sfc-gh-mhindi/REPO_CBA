/*
=======================================================================
TERADATA COUNT QUERIES FOR MISSING BTEQ TABLES
=======================================================================
This script performs count(1) queries on all 14 available tables that 
were found in the K_ databases during table availability analysis.

Analysis Results: 14 available tables out of 46 total (30.4%)
Generated from: table_availability_analysis.md
Date: 2025-08-25 11:41:24

Purpose:
- Get baseline record counts from Teradata source tables
- Used for data validation after migration to Snowflake Iceberg tables  
- Compare with post-migration counts to ensure data integrity

Schema Coverage:
- K_PDCBODS: 6 tables (100% coverage)
- K_PDGRD: 1 table (25% coverage) 
- K_PDPATY: 1 table (100% coverage)
- K_PDSECURITY: 1 table (100% coverage)
- K_STAR_CAD_PROD_DATA: 5 tables (20% coverage)
=======================================================================
*/

-- Main COUNT query using UNION ALL for all 14 available tables
-- Using K_ prefixed database names where the tables actually exist in Teradata
SELECT 
    'K_PDCBODS' as SOURCE_SCHEMA,
    'ACCT_MSTR_CYT_DATA' as TABLE_NAME,
    COUNT(1) as RECORD_COUNT,
    CURRENT_TIMESTAMP as COUNT_TIMESTAMP
FROM K_PDCBODS.ACCT_MSTR_CYT_DATA

UNION ALL

SELECT 
    'K_PDCBODS' as SOURCE_SCHEMA,
    'BUSN_PTNR' as TABLE_NAME,
    COUNT(1) as RECORD_COUNT,
    CURRENT_TIMESTAMP as COUNT_TIMESTAMP
FROM K_PDCBODS.BUSN_PTNR

UNION ALL

SELECT 
    'K_PDCBODS' as SOURCE_SCHEMA,
    'CBA_FNCL_SERV_GL_DATA' as TABLE_NAME,
    COUNT(1) as RECORD_COUNT,
    CURRENT_TIMESTAMP as COUNT_TIMESTAMP
FROM K_PDCBODS.CBA_FNCL_SERV_GL_DATA

UNION ALL

SELECT 
    'K_PDCBODS' as SOURCE_SCHEMA,
    'MSTR_CNCT_BALN_TRNF_PRTP' as TABLE_NAME,
    COUNT(1) as RECORD_COUNT,
    CURRENT_TIMESTAMP as COUNT_TIMESTAMP
FROM K_PDCBODS.MSTR_CNCT_BALN_TRNF_PRTP

UNION ALL

SELECT 
    'K_PDCBODS' as SOURCE_SCHEMA,
    'MSTR_CNCT_MSTR_DATA_GENL' as TABLE_NAME,
    COUNT(1) as RECORD_COUNT,
    CURRENT_TIMESTAMP as COUNT_TIMESTAMP
FROM K_PDCBODS.MSTR_CNCT_MSTR_DATA_GENL

UNION ALL

SELECT 
    'K_PDCBODS' as SOURCE_SCHEMA,
    'MSTR_CNCT_PRXY_ACCT' as TABLE_NAME,
    COUNT(1) as RECORD_COUNT,
    CURRENT_TIMESTAMP as COUNT_TIMESTAMP
FROM K_PDCBODS.MSTR_CNCT_PRXY_ACCT

UNION ALL

SELECT 
    'K_PDGRD' as SOURCE_SCHEMA,
    'GRD_RPRT_CALR_FNYR' as TABLE_NAME,
    COUNT(1) as RECORD_COUNT,
    CURRENT_TIMESTAMP as COUNT_TIMESTAMP
FROM K_PDGRD.GRD_RPRT_CALR_FNYR

UNION ALL

SELECT 
    'K_PDPATY' as SOURCE_SCHEMA,
    'ACCT_PATY' as TABLE_NAME,
    COUNT(1) as RECORD_COUNT,
    CURRENT_TIMESTAMP as COUNT_TIMESTAMP
FROM K_PDPATY.ACCT_PATY

UNION ALL

SELECT 
    'K_PDSECURITY' as SOURCE_SCHEMA,
    'ROW_LEVL_SECU_USER_PRFL' as TABLE_NAME,
    COUNT(1) as RECORD_COUNT,
    CURRENT_TIMESTAMP as COUNT_TIMESTAMP
FROM K_PDSECURITY.ROW_LEVL_SECU_USER_PRFL

UNION ALL

SELECT 
    'K_STAR_CAD_PROD_DATA' as SOURCE_SCHEMA,
    'ACCT_BASE' as TABLE_NAME,
    COUNT(1) as RECORD_COUNT,
    CURRENT_TIMESTAMP as COUNT_TIMESTAMP
FROM K_STAR_CAD_PROD_DATA.ACCT_BASE

UNION ALL

SELECT 
    'K_STAR_CAD_PROD_DATA' as SOURCE_SCHEMA,
    'ACCT_OFFR' as TABLE_NAME,
    COUNT(1) as RECORD_COUNT,
    CURRENT_TIMESTAMP as COUNT_TIMESTAMP
FROM K_STAR_CAD_PROD_DATA.ACCT_OFFR

UNION ALL

SELECT 
    'K_STAR_CAD_PROD_DATA' as SOURCE_SCHEMA,
    'ACCT_PDCT' as TABLE_NAME,
    COUNT(1) as RECORD_COUNT,
    CURRENT_TIMESTAMP as COUNT_TIMESTAMP
FROM K_STAR_CAD_PROD_DATA.ACCT_PDCT

UNION ALL

SELECT 
    'K_STAR_CAD_PROD_DATA' as SOURCE_SCHEMA,
    'DERV_PRTF_ACCT_REL' as TABLE_NAME,
    COUNT(1) as RECORD_COUNT,
    CURRENT_TIMESTAMP as COUNT_TIMESTAMP
FROM K_STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_REL

UNION ALL

SELECT 
    'K_STAR_CAD_PROD_DATA' as SOURCE_SCHEMA,
    'MAP_SAP_INVL_PDCT' as TABLE_NAME,
    COUNT(1) as RECORD_COUNT,
    CURRENT_TIMESTAMP as COUNT_TIMESTAMP
FROM K_STAR_CAD_PROD_DATA.MAP_SAP_INVL_PDCT

ORDER BY SOURCE_SCHEMA, TABLE_NAME;

/*
=======================================================================
EXPECTED OUTPUT FORMAT:
=======================================================================
SOURCE_SCHEMA         | TABLE_NAME              | RECORD_COUNT | COUNT_TIMESTAMP
---------------------|-------------------------|--------------|------------------
K_PDCBODS            | ACCT_MSTR_CYT_DATA     | 123,456      | 2025-08-25 12:00:00
K_PDCBODS            | BUSN_PTNR              | 789,123      | 2025-08-25 12:00:00
K_STAR_CAD_PROD_DATA | ACCT_BASE              | 456,789      | 2025-08-25 12:00:00
...                  | ...                     | ...          | ...

=======================================================================
USAGE INSTRUCTIONS:
=======================================================================
1. Execute this script in Teradata SQL Assistant or your preferred TD client
2. Export results to CSV/Excel for comparison with post-migration counts
3. Use results to validate successful data migration 
4. Compare with Snowflake counts after running P_MIGRATE_MISSING_BTEQ_TABLES()

=======================================================================
POST-MIGRATION VALIDATION QUERY (to run in Snowflake):
=======================================================================
-- Use this query in Snowflake after migration to compare counts:

SELECT 
    'PDCBODS' as TARGET_SCHEMA,
    'ACCT_MSTR_CYT_DATA' as TABLE_NAME,
    COUNT(1) as RECORD_COUNT,
    CURRENT_TIMESTAMP as COUNT_TIMESTAMP
FROM NPD_D12_DMN_GDWMIG_IBRG.PDCBODS.ACCT_MSTR_CYT_DATA

UNION ALL

SELECT 
    'PDCBODS' as TARGET_SCHEMA, 
    'BUSN_PTNR' as TABLE_NAME,
    COUNT(1) as RECORD_COUNT,
    CURRENT_TIMESTAMP as COUNT_TIMESTAMP
FROM NPD_D12_DMN_GDWMIG_IBRG.PDCBODS.BUSN_PTNR

-- ... (continue for all 14 tables using NPD_D12_DMN_GDWMIG_IBRG.<schema>.<table> format)

ORDER BY TARGET_SCHEMA, TABLE_NAME;

=======================================================================
TABLE MAPPING REFERENCE:
=======================================================================
Teradata Source (K_ DBs)           →  Snowflake Target
---------------------------------  →  ------------------------------------------
K_PDCBODS.*                       →  NPD_D12_DMN_GDWMIG_IBRG.PDCBODS.*
K_PDGRD.*                         →  NPD_D12_DMN_GDWMIG_IBRG.PDGRD.*
K_PDPATY.*                        →  NPD_D12_DMN_GDWMIG_IBRG.PDPATY.*
K_PDSECURITY.*                    →  NPD_D12_DMN_GDWMIG_IBRG.PDSECURITY.*
K_STAR_CAD_PROD_DATA.*            →  NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.*

=======================================================================
*/ 