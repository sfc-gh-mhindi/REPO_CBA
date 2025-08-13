USE DATABASE GDW1;

CREATE SCHEMA IF NOT EXISTS GDW1.CSE4_STG;
CREATE SCHEMA IF NOT EXISTS GDW1.CSE4_CTL;
CREATE SCHEMA IF NOT EXISTS GDW1.PDDSTG;
CREATE SCHEMA IF NOT EXISTS GDW1.PVTECH;
CREATE SCHEMA IF NOT EXISTS GDW1.STAR_CAD_PROD_DATA;

CREATE OR REPLACE TABLE GDW1.CSE4_STG.REJT_CPL_BUS_APP
(	PL_APP_ID VARCHAR(12),
NOMINATED_BRANCH_ID VARCHAR(12),
PL_PACKAGE_CAT_ID VARCHAR(12),
ETL_D TIMESTAMP /*** SSC-FDM-OR0042 - DATE TYPE COLUMN HAS A DIFFERENT BEHAVIOR IN SNOWFLAKE. ***/,
ORIG_ETL_D TIMESTAMP /*** SSC-FDM-OR0042 - DATE TYPE COLUMN HAS A DIFFERENT BEHAVIOR IN SNOWFLAKE. ***/,
EROR_C VARCHAR(255)
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 14,  "patch": "0.0" }, "attributes": {  "component": "oracle",  "convertedOn": "08/05/2025",  "domain": "nextpathway" }}'
;
	
CREATE OR REPLACE TABLE GDW1.CSE4_CTL.RUN_STRM_ETL_D
(	"RS_M" VARCHAR(255),
"ETL_D" TIMESTAMP /*** SSC-FDM-OR0042 - DATE TYPE COLUMN HAS A DIFFERENT BEHAVIOR IN SNOWFLAKE. ***/
  )
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 14,  "patch": "0.0" }, "attributes": {  "component": "oracle",  "convertedOn": "08/05/2025",  "domain": "nextpathway" }}'
;

CREATE OR REPLACE TABLE CSE4_CTL.RUN_STRM_TMPL
(	RUN_STRM_C VARCHAR(255) NOT NULL,
RUN_STRM_X VARCHAR(255) NOT NULL,
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

create or replace TABLE GDW1.CSE4_CTL.STEP_OCCR (
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
	

create or replace TABLE GDW1.PDDSTG.TMP_APPT_DEPT (
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



create or replace TABLE GDW1.PDDSTG.TMP_APPT_PDCT (
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



create or replace view GDW1.PVTECH.ACCT_APPT_PDCT(
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
	GDW1_IBRG."starcadproddata"."acct_appt_pdct";
	
	
create or replace view GDW1.PVTECH.APPT_DEPT(
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
	"appt_i",
	"dept_role_c",
	"efft_d",
	"dept_i",
	"expy_d",
	"pros_key_efft_i",
	"pros_key_expy_i",
	"eror_seqn_i",
	"brch_n",
	"srce_syst_c",
	"chng_reas_c"
FROM
	GDW1_IBRG."starcadproddata"."appt_dept";
	

create or replace view GDW1.PVTECH.APPT_PDCT(
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
	"appt_pdct_i",
	"appt_qlfy_c",
	"acqr_type_c",
	"acqr_adhc_x",
	"acqr_srce_c",
	"pdct_n",
	"appt_i",
	"srce_syst_c",
	"srce_syst_appt_pdct_i",
	"loan_fndd_meth_c",
	"new_acct_f",
	"brok_paty_i",
	"copy_from_othr_appt_f",
	"efft_d",
	"expy_d",
	"pros_key_efft_i",
	"pros_key_expy_i",
	"eror_seqn_i",
	"job_comm_catg_c",
	"debt_abn_x",
	"debt_busn_m",
	"smpl_appt_f",
	"appt_pdct_catg_c",
	"appt_pdct_durt_c",
	"ases_d"
FROM
	GDW1_IBRG."starcadproddata"."appt_pdct";
	
	
create or replace view GDW1.PVTECH.MAP_CSE_PACK_PDCT_PL(
	"pl_pack_cat_id",
	"pdct_n",
	"efft_d",
	"expy_d"
) COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
 as
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_pack_pdct_pl";
	

CREATE OR REPLACE TABLE GDW1.STAR_CAD_PROD_DATA.UTIL_PROS_ISAC
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