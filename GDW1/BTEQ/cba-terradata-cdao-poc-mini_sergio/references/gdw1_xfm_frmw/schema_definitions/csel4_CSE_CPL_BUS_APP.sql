-- =====================================================
-- Standard Snowflake SQL Implementation
-- =====================================================
CREATE OR REPLACE TABLE CSE_CPL_BUS_APP (
    MOD_TIMESTAMP  VARCHAR(25) NOT NULL COMMENT 'The last modification timestamp',
    PL_APP_ID  VARCHAR(12) NOT NULL COMMENT 'Personal Loan Application ID - Primary business key',
    NOMINATED_BRANCH_ID VARCHAR(12) NOT NULL COMMENT 'Nominated branch identifier',
    PL_PACKAGE_CAT_ID  VARCHAR(12) NOT NULL COMMENT 'Product package category ID'
)
;
-- =====================================================
-- Snowflake Iceberg Implementation  
-- =====================================================

CREATE OR REPLACE ICEBERG TABLE CSE_CPL_BUS_APP (
    MOD_TIMESTAMP  STRING  NOT NULL COMMENT 'The last modification timestamp',
    PL_APP_ID  STRING  NOT NULL COMMENT 'Personal Loan Application ID - Primary business key',
    NOMINATED_BRANCH_ID STRING  NOT NULL COMMENT 'Nominated branch identifier',
    PL_PACKAGE_CAT_ID  STRING  NOT NULL COMMENT 'Product package category ID'
)
;
