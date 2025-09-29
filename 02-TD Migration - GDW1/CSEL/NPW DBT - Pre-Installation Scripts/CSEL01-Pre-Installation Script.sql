
use role r_dev_npd_d12_gdwmig;
-- use database npd_d12_dmn_gdwmig;
-- use schema migration_tracking;
use warehouse wh_usr_npd_d12_gdwmig_001;


-- CREATE DATABASE IF NOT EXISTS NPD_D12_DMN_GDWMIG_IBRG; //NPD_D12_DMN_GDWMIG_IBRG;
-- CREATE DATABASE IF NOT EXISTS NPD_D12_DMN_GDWMIG; //GDW1_STG; 
-- CREATE DATABASE IF NOT EXISTS NPD_D12_DMN_GDWMIG; //GDW1;
-- CREATE SCHEMA IF NOT EXISTS NPD_D12_DMN_GDWMIG.cse4_ctl; // GDW1.cse4_ctl;
-- CREATE SCHEMA IF NOT EXISTS GDW1_STG.files;
-- CREATE SCHEMA IF NOT EXISTS GDW1_STG.datasets;
-- CREATE WAREHOUSE IF NOT EXISTS DBT WITH WAREHOUSE_SIZE = 'MEDIUM';



USE DATABASE NPD_D12_DMN_GDWMIG;--commbankdb ;
-- CREATE SCHEMA IF NOT EXISTS FILES; will use NPD_D12_DMN_GDWMIG.TMP
-- CREATE SCHEMA IF NOT EXISTS DATASETS; will use NPD_D12_DMN_GDWMIG.TMP

-- CREATE SCHEMA IF NOT EXISTS CSE4_STG ; will use NPD_D12_DMN_GDWMIG.TMP
-- CREATE SCHEMA IF NOT EXISTS CSE3_CTL; will use NPD_D12_DMN_GDWMIG.TMP
--- Tables used.

CREATE OR REPLACE TABLE /*CSE4_STG*/ NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.REJT_CPL_BUS_APP (
    PL_APP_ID            VARCHAR(12),
    NOMINATED_BRANCH_ID  VARCHAR(12),
    PL_PACKAGE_CAT_ID    VARCHAR(12),
    ORIG_ETL_D           DATE,
	EROR_C               VARCHAR(12)
);




--- FILES schema for the sequentials files 



-------gdw1.sql .start
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.REJT_CPL_BUS_APP
(	PL_APP_ID VARCHAR(12),
NOMINATED_BRANCH_ID VARCHAR(12),
PL_PACKAGE_CAT_ID VARCHAR(12),
ETL_D TIMESTAMP /*** SSC-FDM-OR0042 - DATE TYPE COLUMN HAS A DIFFERENT BEHAVIOR IN SNOWFLAKE. ***/,
ORIG_ETL_D TIMESTAMP /*** SSC-FDM-OR0042 - DATE TYPE COLUMN HAS A DIFFERENT BEHAVIOR IN SNOWFLAKE. ***/,
EROR_C VARCHAR(255)
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 14,  "patch": "0.0" }, "attributes": {  "component": "oracle",  "convertedOn": "08/05/2025",  "domain": "nextpathway" }}'
;
	
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.RUN_STRM_ETL_D
(	"RS_M" VARCHAR(255),
"ETL_D" TIMESTAMP /*** SSC-FDM-OR0042 - DATE TYPE COLUMN HAS A DIFFERENT BEHAVIOR IN SNOWFLAKE. ***/
  )
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 14,  "patch": "0.0" }, "attributes": {  "component": "oracle",  "convertedOn": "08/05/2025",  "domain": "nextpathway" }}'
;

CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.RUN_STRM_TMPL
(
    RUN_STRM_C VARCHAR(255) NOT NULL,
    RUN_STRM_X VARCHAR(255), --NOT NULL commented by MH since not traced in solution,
    RUN_STRM_PARM_PJCT_HOME_FLDR_M VARCHAR(500),
    RUN_STRM_PARM_SYST_M VARCHAR(500),
    RUN_STRM_ON_HOLD_F CHAR(1),
    RUN_STRM_ABRT_F CHAR(1),
    RUN_STRM_ACTV_F CHAR(1) NOT NULL,
    RECD_CRAT_S TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL,
    SYST_C VARCHAR(30)
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 14,  "patch": "0.0" }, "attributes": {  "component": "oracle",  "convertedOn": "08/05/2025",  "domain": "nextpathway" }}'
;

create or replace TABLE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.STEP_OCCR (
	STEP_OCCR_ID VARCHAR(255),
	RUN_STRM_OCCR_ID VARCHAR(255),
	RUN_STRM_C VARCHAR(255),
	STEP_C VARCHAR(255),
	STEP_STUS_C VARCHAR(50),
	STEP_OCCR_ISRT_ROW_CNT NUMBER(38,0),
	STEP_OCCR_UPDT_ROW_CNT NUMBER(38,0),
	STEP_OCCR_FAIL_ROW_CNT NUMBER(38,0),
	STEP_OCCR_RSRT_VALU VARCHAR(255),
	STEP_OCCR_STRT_S TIMESTAMP_LTZ(9),
	STEP_OCCR_END_S TIMESTAMP_LTZ(9),
	RECD_CRAT_S TIMESTAMP_LTZ(9),
	STEP_SQNO NUMBER(38,0)
);
	

create or replace TABLE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.TMP_APPT_DEPT (
	APPT_I VARCHAR(255) NOT NULL COMMENT 'Application Identifier',
	DEPT_ROLE_C VARCHAR(5) NOT NULL COMMENT 'Department Role Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	DEPT_I VARCHAR(255) NOT NULL COMMENT 'Department Identifier',
	EXPY_D DATE NOT NULL DEFAULT CAST('9999-12-31' AS DATE) COMMENT 'Expiry Date',
	PROS_KEY_EFFT_I NUMBER(10,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	PROS_KEY_EXPY_I NUMBER(10,0) COMMENT 'Process Key Expiry Identifier',
	EROR_SEQN_I NUMBER(10,0) COMMENT 'Error Sequence Identifier',
	RUN_STRM VARCHAR(100)
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 14,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"08/05/2025\",  \"domain\": \"nextpathway\" }}';



create or replace TABLE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.TMP_APPT_PDCT (
	APPT_PDCT_I VARCHAR(255) NOT NULL COMMENT 'Application Product Identifier',
	CLP_JOB_FAMILY_CAT_ID VARCHAR(4),
	APPT_QLFY_C VARCHAR(2) COMMENT 'Application Qualifier Code',
	ACQR_TYPE_C VARCHAR(4) COMMENT 'Acquisition Type Code',
	ACQR_ADHC_X VARCHAR(255) COMMENT 'Acquisition Ad Hoc Comment',
	ACQR_SRCE_C VARCHAR(4) COMMENT 'Acquisition Source Code',
	PDCT_N NUMBER(38,0) NOT NULL COMMENT 'Product Number',
	APPT_I VARCHAR(255) NOT NULL COMMENT 'Application Identifier',
	SRCE_SYST_C VARCHAR(3) NOT NULL COMMENT 'Source System Code',
	SRCE_SYST_APPT_PDCT_I VARCHAR(255) NOT NULL COMMENT 'Source System Application Product Identifier',
	LOAN_FNDD_METH_C VARCHAR(4) COMMENT 'Loan Funding Method Code',
	NEW_ACCT_F VARCHAR(1) COMMENT 'New Account Flag',
	BROK_PATY_I VARCHAR(255) COMMENT 'Broker Party Identifier',
	COPY_FROM_OTHR_APPT_F VARCHAR(1) DEFAULT 'N' COMMENT 'Copy From Other Application Flag',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL DEFAULT CAST('9999-12-31' AS DATE) COMMENT 'Expiry date',
	PROS_KEY_EFFT_I NUMBER(10,0) NOT NULL COMMENT 'Process Key Effective Identfier',
	PROS_KEY_EXPY_I NUMBER(10,0) COMMENT 'Process Key Expiry Identifier',
	EROR_SEQN_I NUMBER(10,0) COMMENT 'Error Sequence Identifier',
	DEBT_ABN_X VARCHAR(255),
	DEBT_BUSN_M VARCHAR(40),
	SMPL_APPT_F VARCHAR(1),
	RUN_STRM VARCHAR(100),
	APPT_PDCT_CATG_C VARCHAR(4),
	APPT_PDCT_DURT_C VARCHAR(4),
	ASES_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 14,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"08/05/2025\",  \"domain\": \"nextpathway\" }}';



create or replace view NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.ACCT_APPT_PDCT(
	"acct_i",
	"appt_pdct_i",
	"rel_type_c",
	"efft_d",
	"expy_d",
	"pros_key_efft_i",
	"pros_key_expy_i",
	"eror_seqn_i"
) COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
 as
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.starcadproddata.acct_appt_pdct;
	
	
create or replace view NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.APPT_DEPT(
	APPT_I,
	DEPT_ROLE_C,
	EFFT_D,
	DEPT_I,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	BRCH_N,
	SRCE_SYST_C,
	CHNG_REAS_C
) COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
 as
	SELECT
	appt_i,
	dept_role_c,
	efft_d,
	dept_i,
	expy_d,
	pros_key_efft_i,
	pros_key_expy_i,
	eror_seqn_i,
	brch_n,
	srce_syst_c,
	chng_reas_c
FROM
	NPD_D12_DMN_GDWMIG_IBRG.starcadproddata.appt_dept;
	

create or replace view NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.APPT_PDCT(
	APPT_PDCT_I,
	APPT_QLFY_C,
	ACQR_TYPE_C,
	ACQR_ADHC_X,
	ACQR_SRCE_C,
	PDCT_N,
	APPT_I,
	SRCE_SYST_C,
	SRCE_SYST_APPT_PDCT_I,
	LOAN_FNDD_METH_C,
	NEW_ACCT_F,
	BROK_PATY_I,
	COPY_FROM_OTHR_APPT_F,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	JOB_COMM_CATG_C,
	DEBT_ABN_X,
	DEBT_BUSN_M,
	SMPL_APPT_F,
	APPT_PDCT_CATG_C,
	APPT_PDCT_DURT_C,
	ASES_D
) COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
 as
	SELECT
	appt_pdct_i,
	appt_qlfy_c,
	acqr_type_c,
	acqr_adhc_x,
	acqr_srce_c,
	pdct_n,
	appt_i,
	srce_syst_c,
	srce_syst_appt_pdct_i,
	loan_fndd_meth_c,
	new_acct_f,
	brok_paty_i,
	copy_from_othr_appt_f,
	efft_d,
	expy_d,
	pros_key_efft_i,
	pros_key_expy_i,
	eror_seqn_i,
	job_comm_catg_c,
	debt_abn_x,
	debt_busn_m,
	smpl_appt_f,
	appt_pdct_catg_c,
	appt_pdct_durt_c,
	ases_d
FROM
	NPD_D12_DMN_GDWMIG_IBRG.starcadproddata.appt_pdct;
	
	
create or replace view NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.MAP_CSE_PACK_PDCT_PL(
	pl_pack_cat_id,
	pdct_n,
	efft_d,
	expy_d
) COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
 as
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"NPD_D12_DMN_GDWMIG_IBRG". starcadproddata.map_cse_pack_pdct_pl;
	

CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.UTIL_PROS_ISAC
(
	PROS_KEY_I DECIMAL(10,0) NOT NULL,
	CONV_M CHAR(30),
	CONV_TYPE_M CHAR(4),
	PROS_RQST_S TIMESTAMP(0), -- FORMAT 'yyyy-mm-ddbhh:mi:ss' - FORMAT IN TABLE STAR_CAD_PROD_DATA.UTIL_PROS_ISAC NOT SUPPORTED 
	PROS_LAST_RQST_S TIMESTAMP(0), -- FORMAT 'yyyy-mm-ddbhh:mi:ss' - FORMAT IN TABLE STAR_CAD_PROD_DATA.UTIL_PROS_ISAC NOT SUPPORTED 
	PROS_RQST_Q INTEGER,
	BTCH_RUN_D DATE,
	BTCH_KEY_I DECIMAL(10,0),
	SRCE_SYST_M CHAR(30),
	SRCE_M CHAR(40),
	TRGT_M CHAR(40),
	SUCC_F CHAR(1),
	COMT_F CHAR(1),
	COMT_S TIMESTAMP(0), -- FORMAT 'yyyy-mm-ddbhh:mi:ss' - FORMAT IN TABLE STAR_CAD_PROD_DATA.UTIL_PROS_ISAC NOT SUPPORTED 
	MLTI_LOAD_EFFT_D DATE,
	SYST_S TIMESTAMP(0), -- FORMAT 'yyyy-mm-ddbhh:mi:ss' - FORMAT IN TABLE STAR_CAD_PROD_DATA.UTIL_PROS_ISAC NOT SUPPORTED 
	MLTI_LOAD_COMT_S TIMESTAMP(0), -- FORMAT 'yyyy-mm-ddbhh:mi:ss' - FORMAT IN TABLE STAR_CAD_PROD_DATA.UTIL_PROS_ISAC NOT SUPPORTED 
	SYST_ET_Q INTEGER,
	SYST_UV_Q INTEGER,
	SYST_INS_Q INTEGER,
	SYST_UPD_Q INTEGER,
	SYST_DEL_Q INTEGER,
	SYST_ET_TABL_M VARCHAR(60),
	SYST_UV_TABL_M VARCHAR(60),
	SYST_HEAD_ET_TABL_M VARCHAR(60),
	SYST_HEAD_UV_TABL_M VARCHAR(60),
	SYST_TRLR_ET_TABL_M VARCHAR(60),
	SYST_TRLR_UV_TABL_M VARCHAR(60),
	PREV_PROS_KEY_I DECIMAL(10,0),
	HEAD_RECD_TYPE_C CHAR(8),
	HEAD_FILE_M CHAR(40),
	HEAD_BTCH_RUN_D DATE,
	HEAD_FILE_CRAT_S TIMESTAMP(0), -- FORMAT 'yyyy-mm-ddbhh:mi:ss' - FORMAT IN TABLE STAR_CAD_PROD_DATA.UTIL_PROS_ISAC NOT SUPPORTED 
	HEAD_GENR_PRGM_M CHAR(40),
	HEAD_BTCH_KEY_I DECIMAL(10,0),
	HEAD_PROS_KEY_I DECIMAL(10,0),
	HEAD_PROS_PREV_KEY_I DECIMAL(10,0),
	TRLR_RECD_TYPE_C CHAR(8),
	TRLR_RECD_Q INTEGER,
	TRLR_HASH_TOTL_A DECIMAL(18,4),
	TRLR_COLM_HASH_TOTL_M CHAR(40),
	TRLR_EROR_RECD_Q INTEGER,
	TRLR_FILE_COMT_S TIMESTAMP(0), -- FORMAT 'yyyy-mm-ddbhh:mi:ss' - FORMAT IN TABLE STAR_CAD_PROD_DATA.UTIL_PROS_ISAC NOT SUPPORTED 
	TRLR_RECD_ISRT_Q INTEGER,
	TRLR_RECD_UPDT_Q INTEGER,
	TRLR_RECD_DELT_Q INTEGER
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
;
-------gdw1.sql .end

create or replace TABLE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.CBA_APP__CSEL4__CSELDEV__INPROCESS__CSE_CPL_BUS_APP_CSE_CPL_BUS_APP_20250807__DLY (
    RECORD_TYPE VARCHAR(1),
    MOD_TIMESTAMP VARCHAR(25),
    PL_APP_ID VARCHAR(12),
    NOMINATED_BRANCH_ID VARCHAR(12),
    PL_PACKAGE_CAT_ID VARCHAR(12),
    DUMMY VARCHAR(1)
);


-- =====================================================
-- COMPREHENSIVE FIX FOR NPW DBT PROJECT ERRORS
-- =====================================================
-- This script addresses all missing objects and control data issues
-- Based on error analysis from models execution
-- =====================================================

USE DATABASE NPD_D12_DMN_GDWMIG_IBRG_V;
USE SCHEMA P_V_OUT_001_STD_0;

-- =====================================================
-- PART 1: CREATE MISSING INPROCESS TABLES
-- =====================================================
-- These tables represent external data feeds that should exist
-- before dbt models run. Creating with sample data for testing.

-- Current date INPROCESS table (matches error: 20250807)
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.CBA_APP__CSEL4__CSEL4DEV__INPROCESS__CSE_CPL_BUS_APP_CSE_CPL_BUS_APP_20250807__DLY (
    RECORD_TYPE VARCHAR(1),
    MOD_TIMESTAMP VARCHAR(25),
    PL_APP_ID VARCHAR(12),
    NOMINATED_BRANCH_ID VARCHAR(12),
    PL_PACKAGE_CAT_ID VARCHAR(12),
    DUMMY VARCHAR(1),
    LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- Create the LOOKUPSET table that the mapping transformation needs
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.CBA_APP__CSEL4__CSEL4DEV__LOOKUPSET__MAP_CSE_PACK_PDCT_PL_PL_PACK_CAT_ID__FS (
    PL_PACKAGE_CAT_ID VARCHAR(16777216),
    PDCT_N VARCHAR(16777216),
    LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- PART 3: FIX CONTROL DATA FLAGS
-- =====================================================
-- Reset run stream flags to proper values for dbt processing

-- Check current state first
SELECT 
    'BEFORE_FIX' as status,
    RUN_STRM_C,
    RUN_STRM_ABRT_F,
    RUN_STRM_ACTV_F,
    SYST_C,
    CASE 
        WHEN RUN_STRM_ABRT_F = 'Y' THEN '❌ ABORTED'
        WHEN RUN_STRM_ACTV_F = 'Y' THEN '🚨 ACTIVE (PROBLEMATIC)'
        WHEN RUN_STRM_ACTV_F = 'I' THEN '✅ INACTIVE (READY)'
        ELSE '⚠️ OTHER'
    END as flag_status
FROM NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.RUN_STRM_TMPL 
-- WHERE RUN_STRM_C IN ('CSE_ICE_BUS_DEAL', 'CSE_CPL_BUS_APP')
ORDER BY RUN_STRM_C;

-- Fix the flags for all streams
UPDATE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.RUN_STRM_TMPL 
SET 
    RUN_STRM_ABRT_F = 'N',    -- Not aborted
    RUN_STRM_ACTV_F = 'I'    -- Inactive (ready to run)
WHERE RUN_STRM_C IN (
    'CSE_ICE_BUS_DEAL', 
    'CSE_CPL_BUS_APP',
    'CSE_CCC_BUS_APP_PROD',
    'CSE_COM_BUS_CCL_CHL_COM_APP',
    'CSE_COM_BUS_APP_PROD_CCL_PL_APP_PROD',
    'CSE_COM_CPO_BUS_NCPR_CLNT',
    'CSE_L4_PRE_PROC'
) AND SYST_C = 'CSEL4';
