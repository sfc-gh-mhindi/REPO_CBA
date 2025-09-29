use role ACCOUNTADMIN;
USE DATABASE PS_CLD_RW;

CREATE OR REPLACE VIEW PVTECH.ACCT_BASE
(
        ACCT_I,
        SRCE_SYST_ACCT_N,
        OPEN_D,
        ACCT_CLSE_REAS_C,
        CLSE_D,
        BUSN_UNIT_BRCH_N,
        ORIG_I,
        BUSN_UNIT_I,
        SRCE_SYST_C,
        ACCT_N,
        ACCT_QLFY_C,
        CNCY_C,
        ACCT_OBTN_TYPE_C,
        ACCT_CATG_TYPE_C,
        ACCT_CLAS_TYPE_C,
        PROS_KEY_EFFT_I,
        PROS_KEY_EXPY_I,
        EROR_SEQN_I,
        ROW_SECU_ACCS_C,
        ISO_CNTY_C,
        BANK_N,
        EFFT_D,
        EXPY_D,
        RECORD_DELETED_FLAG,
        CTL_ID,
        PROCESS_NAME,
        PROCESS_ID,
        UPDATE_PROCESS_NAME,
        UPDATE_PROCESS_ID
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
AS
SELECT
        ACCT_I,
        SRCE_SYST_ACCT_N,
        OPEN_D,
        ACCT_CLSE_REAS_C,
        CLSE_D,
        BUSN_UNIT_BRCH_N,
        ORIG_I,
        BUSN_UNIT_I,
        SRCE_SYST_C,
        ACCT_N,
        ACCT_QLFY_C,
        CNCY_C,
        ACCT_OBTN_TYPE_C,
        ACCT_CATG_TYPE_C,
        ACCT_CLAS_TYPE_C,
        PROS_KEY_EFFT_I,
        PROS_KEY_EXPY_I,
        EROR_SEQN_I,
        ROW_SECU_ACCS_C,
        ISO_CNTY_C,
        BANK_N,
        EFFT_D,
        EXPY_D,
        RECORD_DELETED_FLAG,
        CTL_ID,
        PROCESS_NAME,
        PROCESS_ID,
        UPDATE_PROCESS_NAME,
        UPDATE_PROCESS_ID
  FROM (
		SELECT
			ACCT_I,
			SRCE_SYST_ACCT_N,
			OPEN_D,
			ACCT_CLSE_REAS_C,
			CLSE_D,
			BUSN_UNIT_BRCH_N,
			ORIG_I,
			BUSN_UNIT_I,
			SRCE_SYST_C,
			ACCT_N,
			ACCT_QLFY_C,
			CNCY_C,
			ACCT_OBTN_TYPE_C,
			ACCT_CATG_TYPE_C,
			ACCT_CLAS_TYPE_C,
			PROS_KEY_EFFT_I,
			PROS_KEY_EXPY_I,
			EROR_SEQN_I,
			CASE
				WHEN ACCT_I ILIKE 'FMS%'
					THEN 1 ELSE ROW_SECU_ACCS_C
			END AS ROW_SECU_ACCS_C,
			ISO_CNTY_C,
			BANK_N,
			EFFT_D,
			EXPY_D,
			RECORD_DELETED_FLAG,
			CTL_ID,
			PROCESS_NAME,
			PROCESS_ID,
			UPDATE_PROCESS_NAME,
			UPDATE_PROCESS_ID
FROM
			STAR_CAD_PROD_DATA.ACCT_BASE
        ) ACCT_BASE
  WHERE
 ((
/* Start - RLS */
        COALESCE(ROW_SECU_ACCS_C,0) = 0 OR GETBIT( (
		SELECT
			ROW_SECU_PRFL_C
	FROM
			PVSECURITY.ROW_LEVL_SECU_USER_PRFL
	WHERE
			UPPER(RTRIM( USERNAME)) = UPPER(RTRIM(CURRENT_USER))
),ROW_SECU_ACCS_C
) = 1
/* End - RLS */
));

/* <sc-table> STAR_CAD_PROD_DATA.ACCT_BASE </sc-table> */
			--** SSC-FDM-TD0024 - SET TABLE FUNCTIONALITY NOT SUPPORTED. TABLE MIGHT HAVE DUPLICATE ROWS **
CREATE OR REPLACE TABLE STAR_CAD_PROD_DATA.ACCT_BASE
			(
        ACCT_I VARCHAR(255) NOT NULL COMMENT 'Account Identifier',
        SRCE_SYST_ACCT_N VARCHAR(255) COMMENT 'Source System Account Number',
        OPEN_D DATE COMMENT 'Account Open Date',
        ACCT_CLSE_REAS_C CHAR(5) COMMENT 'Account Close Reason Code',
        CLSE_D DATE COMMENT 'Account Closed Date',
        BUSN_UNIT_BRCH_N CHAR(4) COMMENT 'Business Unit Branch Number',
        ORIG_I CHAR(5) NOT NULL COMMENT 'Origin Identifier',
        BUSN_UNIT_I VARCHAR(5) NOT NULL COMMENT 'Business Unit Identifier',
        SRCE_SYST_C CHAR(3) NOT NULL COMMENT 'Source System Code',
        ACCT_N CHAR(16) COMMENT 'Account Number',
        ACCT_QLFY_C CHAR(2) NOT NULL COMMENT 'Account Qualifier Code',
        CNCY_C CHAR(3) COMMENT 'Currency Code',
        ACCT_OBTN_TYPE_C CHAR(4) NOT NULL DEFAULT 'UNKN' COMMENT 'Account Obtained Type Code',
        ACCT_CATG_TYPE_C CHAR(4) NOT NULL DEFAULT 'FNCL' COMMENT 'Account Category Type Code',
        ACCT_CLAS_TYPE_C CHAR(4) NOT NULL DEFAULT 'ACCT' COMMENT 'Account Class Type Code',
        PROS_KEY_EFFT_I DECIMAL(10,0) COMMENT 'Process Key Effective Identifier',
        PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Process Key Expiry Identifier',
        EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error Sequence Identifier',
        ROW_SECU_ACCS_C INTEGER NOT NULL DEFAULT 0 COMMENT 'Row Security Access Code',
        ISO_CNTY_C CHAR(2) COMMENT 'ISO Country Code',
        BANK_N CHAR(2) COMMENT 'Bank Number',
        EFFT_D DATE COMMENT 'Effective Date',
        EXPY_D DATE COMMENT 'Expiry Date',
        RECORD_DELETED_FLAG BYTEINT NOT NULL DEFAULT 0 COMMENT 'Record Deleted Flag',
        CTL_ID SMALLINT NOT NULL DEFAULT 0 COMMENT 'Ctl Id',
        PROCESS_NAME CHAR(30) NOT NULL DEFAULT 'NON_CTLFW                     ' COMMENT 'Process Name',
        PROCESS_ID INTEGER NOT NULL DEFAULT 0 COMMENT 'Process Id',
        UPDATE_PROCESS_NAME CHAR(30) COMMENT 'Update Process Name',
        UPDATE_PROCESS_ID INTEGER COMMENT 'Update Process Id'
			)
			COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
			;

CREATE OR REPLACE VIEW PVTECH.ACCT_OFFR
(
        ACCT_I,
        OFFR_I,
        EFFT_D,
        SRCE_SYST_C,
        EXPY_D,
        PROS_KEY_EFFT_I,
        PROS_KEY_EXPY_I,
        ROW_SECU_ACCS_C,
        ACCT_OFFR_STRT_D,
        ACCT_OFFR_END_D
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
AS
SELECT
        ACCT_I,
        OFFR_I,
        EFFT_D,
        SRCE_SYST_C,
        EXPY_D,
        PROS_KEY_EFFT_I,
        PROS_KEY_EXPY_I,
        ROW_SECU_ACCS_C,
        ACCT_OFFR_STRT_D,
        ACCT_OFFR_END_D
  FROM
        STAR_CAD_PROD_DATA.ACCT_OFFR
 WHERE
(
/* Start - RLS */
        COALESCE(ROW_SECU_ACCS_C,0) = 0 OR GETBIT( (
		SELECT
			ROW_SECU_PRFL_C
	FROM
			PVSECURITY.ROW_LEVL_SECU_USER_PRFL
	WHERE
			UPPER(RTRIM( USERNAME)) = UPPER(RTRIM(CURRENT_USER))
),ROW_SECU_ACCS_C
) = 1
/* End - RLS */
);

/* <sc-table> STAR_CAD_PROD_DATA.ACCT_OFFR </sc-table> */
			--** SSC-FDM-TD0024 - SET TABLE FUNCTIONALITY NOT SUPPORTED. TABLE MIGHT HAVE DUPLICATE ROWS **
CREATE OR REPLACE TABLE STAR_CAD_PROD_DATA.ACCT_OFFR
(
        ACCT_I VARCHAR(255) NOT NULL COMMENT 'Account Identifier',
        OFFR_I VARCHAR(255) NOT NULL COMMENT 'Offer Identifier',
        EFFT_D DATE NOT NULL COMMENT 'Effective Date',
        SRCE_SYST_C CHAR(3) NOT NULL COMMENT 'Source System Code',
        EXPY_D DATE NOT NULL DEFAULT '9999-12-31' :: DATE COMMENT 'Expiry Date',
        PROS_KEY_EFFT_I DECIMAL(10,0) COMMENT 'Process Key Effective Identifier',
        PROS_KEY_EXPY_I DECIMAL(10,0) DEFAULT NULL COMMENT 'Process Key Expiry Identifier' ,
        ROW_SECU_ACCS_C INTEGER NOT NULL COMMENT 'Row Security Access Code',
        ACCT_OFFR_STRT_D DATE DEFAULT NULL COMMENT 'ACCOUNT OFFER START DATE',
        ACCT_OFFR_END_D DATE DEFAULT NULL COMMENT 'ACCOUNT OFFER END DATE'
			)
			COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
			;

			CREATE OR REPLACE VIEW PVTECH.ACCT_PATY
			(
			        PATY_I,
			        ACCT_I,
			        PATY_ACCT_REL_C,
			        REL_LEVL_C,
			        REL_REAS_C,
			        REL_STUS_C,
			        SRCE_SYST_C,
			        EFFT_D,
			        EXPY_D,
			        PROS_KEY_EFFT_I,
			        PROS_KEY_EXPY_I,
			        ROW_SECU_ACCS_C
			)
			COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
			AS
			SELECT
        PATY_I,
        ACCT_I,
        PATY_ACCT_REL_C,
        REL_LEVL_C,
        REL_REAS_C,
        REL_STUS_C,
        SRCE_SYST_C,
        EFFT_D,
        EXPY_D,
        PROS_KEY_EFFT_I,
        PROS_KEY_EXPY_I,
        ROW_SECU_ACCS_C
  FROM (
		SELECT
			PATY_I,
			ACCT_I,
			PATY_ACCT_REL_C,
			REL_LEVL_C,
			REL_REAS_C,
			REL_STUS_C,
			SRCE_SYST_C,
			EFFT_D,
			EXPY_D,
			PROS_KEY_EFFT_I,
			PROS_KEY_EXPY_I,
			CASE
				WHEN ACCT_I ILIKE 'FMS%'
					THEN 1 ELSE ROW_SECU_ACCS_C
			END AS ROW_SECU_ACCS_C
FROM
			PDPATY.ACCT_PATY
        ) ACCT_PATY
 WHERE
        (
/* Start - RLS */
        COALESCE(ROW_SECU_ACCS_C,0) = 0 OR GETBIT( (
		SELECT
			ROW_SECU_PRFL_C
	FROM
			PVSECURITY.ROW_LEVL_SECU_USER_PRFL
	WHERE
			UPPER(RTRIM( USERNAME)) = UPPER(RTRIM(CURRENT_USER))
),ROW_SECU_ACCS_C
) = 1
/* End - RLS */
);

/* <sc-table> PDPATY.ACCT_PATY </sc-table> */
			--** SSC-FDM-TD0024 - SET TABLE FUNCTIONALITY NOT SUPPORTED. TABLE MIGHT HAVE DUPLICATE ROWS **
			CREATE OR REPLACE TABLE PDPATY.ACCT_PATY
			(
        PATY_I VARCHAR(255) NOT NULL COMMENT 'Party Identifier',
        ACCT_I VARCHAR(255) NOT NULL COMMENT 'Account Identifier',
        PATY_ACCT_REL_C CHAR(6) NOT NULL COMMENT 'Party Account Relationship Code',
        REL_LEVL_C CHAR(5) COMMENT 'Relation Level Code',
        REL_REAS_C CHAR(5) COMMENT 'Relation Reason Code',
        REL_STUS_C CHAR(1) COMMENT 'Relation Status Code',
        SRCE_SYST_C CHAR(3) COMMENT 'Source System Code',
        EFFT_D DATE NOT NULL COMMENT 'Effective Date',
        EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
        PROS_KEY_EFFT_I INTEGER NOT NULL DEFAULT 0 COMMENT 'Process Key Effective Identifier',
        PROS_KEY_EXPY_I INTEGER DEFAULT NULL COMMENT 'Process Key Expiry Identifier',
        ROW_SECU_ACCS_C INTEGER NOT NULL DEFAULT 0 COMMENT 'Row Security Access Code'
			)
			COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
			;

			CREATE OR REPLACE VIEW PVTECH.ACCT_PDCT
			(
			        ACCT_I,
			        EFFT_D,
			        EXPY_D,
			        PDCT_N,
			        PROS_KEY_EFFT_I,
			        PROS_KEY_EXPY_I,
			        EROR_SEQN_I,
			        ROW_SECU_ACCS_C,
			        MFTR_PDCT_I,
			        SRCE_SYST_C,
			        RECORD_DELETED_FLAG,
			        CTL_ID,
			        PROCESS_NAME,
			        PROCESS_ID,
			        UPDATE_PROCESS_NAME,
			        UPDATE_PROCESS_ID
			)
			COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
			AS
			SELECT
        ACCT_I,
        EFFT_D,
        EXPY_D,
        PDCT_N,
        PROS_KEY_EFFT_I,
        PROS_KEY_EXPY_I,
        EROR_SEQN_I,
        ROW_SECU_ACCS_C,
        MFTR_PDCT_I,
        SRCE_SYST_C,
        RECORD_DELETED_FLAG,
        CTL_ID,
        PROCESS_NAME,
        PROCESS_ID,
        UPDATE_PROCESS_NAME,
        UPDATE_PROCESS_ID
  FROM (
		SELECT
			ACCT_I,
			EFFT_D,
			EXPY_D,
			PDCT_N,
			PROS_KEY_EFFT_I,
			PROS_KEY_EXPY_I,
			EROR_SEQN_I,
			CASE
				WHEN ACCT_I ILIKE 'FMS%'
					THEN 1 ELSE ROW_SECU_ACCS_C
			END AS ROW_SECU_ACCS_C,
			MFTR_PDCT_I,
			SRCE_SYST_C,
			RECORD_DELETED_FLAG,
			CTL_ID,
			PROCESS_NAME,
			PROCESS_ID,
			UPDATE_PROCESS_NAME,
			UPDATE_PROCESS_ID
FROM
			STAR_CAD_PROD_DATA.ACCT_PDCT
        ) ACCT_PDCT
  WHERE
 RECORD_DELETED_FLAG = 0
 AND ( (
/* Start - RLS */
        COALESCE(ROW_SECU_ACCS_C,0) = 0 OR GETBIT( (
		SELECT
			ROW_SECU_PRFL_C
	FROM
			PVSECURITY.ROW_LEVL_SECU_USER_PRFL
	WHERE
			UPPER(RTRIM( USERNAME)) = UPPER(RTRIM(CURRENT_USER))
),ROW_SECU_ACCS_C
) = 1
/* End - RLS */
));

/* <sc-table> STAR_CAD_PROD_DATA.ACCT_PDCT </sc-table> */
			--** SSC-FDM-TD0024 - SET TABLE FUNCTIONALITY NOT SUPPORTED. TABLE MIGHT HAVE DUPLICATE ROWS **
			CREATE OR REPLACE TABLE STAR_CAD_PROD_DATA.ACCT_PDCT
			(
        ACCT_I VARCHAR(255) NOT NULL COMMENT 'Account Identifier',
        EFFT_D DATE NOT NULL COMMENT 'Effective Date',
        EXPY_D DATE NOT NULL DEFAULT '9999-12-31' :: DATE COMMENT 'Expiry Date',
        PDCT_N INTEGER NOT NULL COMMENT 'Product Number',
        PROS_KEY_EFFT_I DECIMAL(10,0) COMMENT 'Process Key Effective Identifier',
        PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Process Key Expiry Identifier',
        EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error Sequence Identifier',
        ROW_SECU_ACCS_C INTEGER NOT NULL DEFAULT 0 COMMENT 'Row Security Access Code',
        MFTR_PDCT_I VARCHAR(255) COMMENT 'Manufacturing Product Identifier',
        SRCE_SYST_C CHAR(3) DEFAULT NULL COMMENT 'Source System Code',
        RECORD_DELETED_FLAG BYTEINT NOT NULL DEFAULT 0 COMMENT 'Record Deleted Flag',
        CTL_ID SMALLINT NOT NULL DEFAULT 0 COMMENT 'Ctl Id',
        PROCESS_NAME CHAR(30) NOT NULL DEFAULT 'NON_CTLFW                     ' COMMENT 'Process Name',
        PROCESS_ID INTEGER NOT NULL DEFAULT 0 COMMENT 'Process Id',
        UPDATE_PROCESS_NAME CHAR(30) COMMENT 'Update Process Name',
        UPDATE_PROCESS_ID INTEGER COMMENT 'Update Process Id'
			)
			COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
			;

/* <sc-view> PVTECH.DERV_PRTF_ACCT </sc-view> */
			CREATE OR REPLACE VIEW PVTECH.DERV_PRTF_ACCT
			-- --- ---------- 
			-- Ver Date       Modified By         Description
			-- --- ---------- 
			--
			-- 1.0  26/06/2013 T Jelliffe          Initial Version
			-- 1.1  28/06/2013 T Jelliffe          Use duration persist table
			-- 1.2  12/07/2013 T Jelliffe          Time period reduced 15 to 3 years
			-- 1.3  17/07/2013 T Jelliffe          39 months history range
			-- 1.4  25/07/2013 T Jelliffe          Date join on JOIN_FROM_D and TO_D
			-- 1.5  14/01/2014 H Zak               read from the corresponding 1:1 views over the new relationship tables
			(
			 PERD_D
			,VALD_FROM_D
			,VALD_TO_D
			,EFFT_D
			,EXPY_D
			,ACCT_I
			,INT_GRUP_I
			,DERV_PRTF_CATG_C
			,DERV_PRTF_CLAS_C
			,DERV_PRTF_TYPE_C
			,PTCL_N
			,REL_MNGE_I
			,PRTF_CODE_X
			,SRCE_SYST_C
			,ROW_SECU_ACCS_C

			)
			COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
			AS
			SELECT
        CALR.CALENDAR_DATE AS PERD_D,
        VALD_FROM_D,
        VALD_TO_D,
        EFFT_D,
        EXPY_D,
        ACCT_I,
        INT_GRUP_I,
        DERV_PRTF_CATG_C,
        DERV_PRTF_CLAS_C,
        DERV_PRTF_TYPE_C,
        PTCL_N,
        REL_MNGE_I,
        PRTF_CODE_X,
        SRCE_SYST_C,
        ROW_SECU_ACCS_C
      FROM
        PVTECH.DERV_PRTF_ACCT_REL T1
        INNER JOIN
		PVTECH.CALENDAR CALR
        ON CALR.CALENDAR_DATE BETWEEN T1.VALD_FROM_D AND T1.VALD_TO_D
        AND CALR.CALENDAR_DATE BETWEEN DATEADD(MONTH, -39, (CURRENT_DATE() - EXTRACT(DAY FROM CURRENT_DATE()) +1 ))
        AND DATEADD(MONTH, 1, CURRENT_DATE())
		;

/* <sc-view> PVTECH.DERV_PRTF_ACCT_REL </sc-view> */
		CREATE OR REPLACE VIEW PVTECH.DERV_PRTF_ACCT_REL
		(
		        ACCT_I,
			 INT_GRUP_I,
			 DERV_PRTF_CATG_C,
			 DERV_PRTF_CLAS_C,
			 DERV_PRTF_TYPE_C,
			 VALD_FROM_D,
			 VALD_TO_D,
			 EFFT_D,
			 EXPY_D,
			 PTCL_N,
			 REL_MNGE_I,
			 PRTF_CODE_X,
			 SRCE_SYST_C,
			 ROW_SECU_ACCS_C
		)
		COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
		AS
		SELECT
        ACCT_I,
	 INT_GRUP_I,
	 DERV_PRTF_CATG_C,
	 DERV_PRTF_CLAS_C,
	 DERV_PRTF_TYPE_C,
	 VALD_FROM_D,
	 VALD_TO_D,
	 EFFT_D,
	 EXPY_D,
	 PTCL_N,
	 REL_MNGE_I,
	 PRTF_CODE_X,
	 SRCE_SYST_C,
	 ROW_SECU_ACCS_C
  FROM
        STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_REL

 WHERE (
(
/* Start - RLS */
        COALESCE(ROW_SECU_ACCS_C,0) = 0 OR GETBIT( (
		SELECT
			ROW_SECU_PRFL_C
	FROM
			PVSECURITY.ROW_LEVL_SECU_USER_PRFL
	WHERE
			UPPER(RTRIM( USERNAME)) = UPPER(RTRIM(CURRENT_USER))
),ROW_SECU_ACCS_C
) = 1
/* End - RLS */
)
    );

/* <sc-view> PVTECH.CALENDAR </sc-view> */
			CREATE OR REPLACE VIEW PVTECH.CALENDAR
			COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
			AS
			--** SSC-FDM-0001 - VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
			SELECT
        *
			FROM SYS_CALENDAR.CALENDAR;

 /* <sc-table> STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_REL </sc-table> */
			CREATE OR REPLACE TABLE STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_REL
			(
        ACCT_I VARCHAR(255) NOT NULL COMMENT 'Account Identifier',
        INT_GRUP_I VARCHAR(255) NOT NULL COMMENT 'Interest Group Identifier',
        DERV_PRTF_CATG_C VARCHAR(255) COMMENT 'Derived Portfolio Category Code',
        DERV_PRTF_CLAS_C VARCHAR(255) COMMENT 'Derived Portfolio Class Code',
        DERV_PRTF_TYPE_C CHAR(4) COMMENT 'Derived Portfolio Type Code',
        VALD_FROM_D DATE COMMENT 'Valid From Date',
        VALD_TO_D DATE COMMENT 'Valid To Date',
        EFFT_D DATE NOT NULL COMMENT 'Effective Date',
        EXPY_D DATE NOT NULL DEFAULT '9999-12-31' :: DATE COMMENT 'Expiry Date',
        PTCL_N SMALLINT COMMENT 'Point Of Control Number',
        REL_MNGE_I VARCHAR(255) COMMENT 'Relationship Manager Identifier',
        PRTF_CODE_X VARCHAR(255) COMMENT 'Portfolio Code Description',
        SRCE_SYST_C CHAR(3) COMMENT 'Source System Code',
        ROW_SECU_ACCS_C INTEGER NOT NULL DEFAULT 0 COMMENT 'Row Secuirty Access Code'
			)
			COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
			;

/* <sc-view> PVTECH.DERV_PRTF_PATY_PSST </sc-view> */
			/* Re-place the existing views */
			CREATE OR REPLACE VIEW PVTECH.DERV_PRTF_PATY_PSST
			COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
			AS
			SELECT
        INT_GRUP_I,
        PATY_I,
        JOIN_FROM_D,
        JOIN_TO_D,
        EFFT_D,
        EXPY_D,
        VALD_FROM_D,
        VALD_TO_D,
        REL_C,
        SRCE_SYST_C,
        ROW_SECU_ACCS_C,
        PROS_KEY_I
     FROM
        STAR_CAD_PROD_DATA.DERV_PRTF_PATY_PSST
        -- $LastChangedBy: jelifft $
        -- $LastChangedDate: 2013-10-21 16:39:07 +1100 (Mon, 21 Oct 2013) $
        -- $LastChangedRevision: 12832 $
        ;

/* <sc-table> STAR_CAD_PROD_DATA.DERV_PRTF_PATY_PSST </sc-table> */
        CREATE OR REPLACE TABLE STAR_CAD_PROD_DATA.DERV_PRTF_PATY_PSST
        (
        INT_GRUP_I VARCHAR(255),
        PATY_I VARCHAR(255),
        JOIN_FROM_D DATE,
        JOIN_TO_D DATE,
        EFFT_D DATE,
        EXPY_D DATE,
        VALD_FROM_D DATE,
        VALD_TO_D DATE,
        REL_C CHAR(4),
        SRCE_SYST_C CHAR(3),
        ROW_SECU_ACCS_C INTEGER,
        PROS_KEY_I DECIMAL(10,0))
        COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
        ;

/* <sc-view> PVTECH.GRD_RPRT_CALR_FNYR </sc-view> */
        CREATE OR REPLACE VIEW PVTECH.GRD_RPRT_CALR_FNYR
        COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
        AS
        --** SSC-FDM-0001 - VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
        SELECT
        *
        FROM
        PDGRD.GRD_RPRT_CALR_FNYR;

 /* <sc-table> PDGRD.GRD_RPRT_CALR_FNYR </sc-table> */
        --** SSC-FDM-TD0024 - SET TABLE FUNCTIONALITY NOT SUPPORTED. TABLE MIGHT HAVE DUPLICATE ROWS **
        CREATE OR REPLACE TABLE PDGRD.GRD_RPRT_CALR_FNYR
        (
        FNCL_CALR_D DATE,
        FNCL_DAY_SNAM_M VARCHAR(20),
        FNCL_DAY_LONG_M VARCHAR(20),
        FNCL_DAY_N INTEGER,
        FNCL_DAY_SNCE_1900_N INTEGER,
        FNCL_DATJ_D VARCHAR(7),
        FNCL_NON_WORK_DAY_F VARCHAR(1),
        FNCL_WKND_F VARCHAR(1),
        FNCL_NON_WORK_DAY_TYPE_C VARCHAR(4),
        FNCL_NON_WORK_DAY_M VARCHAR(50),
        FNCL_WEEK_N INTEGER,
        FNCL_RPRT_YEAR_WEEK_N INTEGER,
        FNCL_WEEK_DAY_N INTEGER,
        FNCL_RPRT_WEEK_DAY_N INTEGER,
        FNCL_WEEK_SNCE_1900_N INTEGER,
        FNCL_WEEK_FRST_DAY_D DATE,
        FNCL_WEEK_LAST_DAY_D DATE,
        FNCL_WEEK_DAY_RNGE_X VARCHAR(34),
        FNCL_RPRT_WEEK_DAY_RNGE_X VARCHAR(67),
        FNWK_RPRT_TMPD_STRT_D DATE,
        FNWK_RPRT_TMPD_END_D DATE,
        FNWK_RPRT_TMPD_LABL_M VARCHAR(40),
        FNWK_RPRT_TMPD_LABL_X VARCHAR(255),
        FNCL_RPRT_WEEK_SNCE_1900_N INTEGER,
        FNCL_MNTH_N INTEGER,
        FNCL_MNTH_SNAM_M VARCHAR(20),
        FNCL_MNTH_LONG_M VARCHAR(20),
        FNCL_MNTH_YEAR_SNAM_M VARCHAR(23),
        FNCL_MNTH_YEAR_LONG_M VARCHAR(25),
        FNCL_MNTH_FRST_DAY_D DATE,
        FNCL_MNTH_LAST_DAY_D DATE,
        FNCL_MNTH_DAY_N INTEGER,
        FNCL_MNTH_WEEK_N INTEGER,
        FNCL_MNTH_SNCE_1900_N INTEGER,
        FNCL_QRTR_N INTEGER,
        FNCL_QRTR_MNTH_N INTEGER,
        FNCL_QRTR_SNAM_M VARCHAR(5),
        FNCL_QRTR_LONG_M VARCHAR(11),
        FNCL_QRTR_SNCE_1900_N INTEGER,
        FNCL_HFYR_N BYTEINT,
        FNCL_HFYR_SNAM_M VARCHAR(7),
        FNCL_HFYR_LONG_M VARCHAR(11),
        FNCL_HFYR_SNCE_1900_N INTEGER,
        FNCL_YEAR_N INTEGER,
        FNYR_RPRT_TMPD_LABL_M VARCHAR(40),
        FNCL_YEAR_SNAM_M VARCHAR(5),
        FNCL_YEAR_LONG_M VARCHAR(9),
        FNYR_RPRT_TMPD_LABL_X VARCHAR(255),
        FNYR_RPRT_TMPD_STRT_D DATE,
        FNYR_RPRT_TMPD_END_D DATE,
        FNCL_LEAP_YEAR_F VARCHAR(1),
        FNCL_YEAR_MONTH_N INTEGER,
        FNCL_RPRT_MNTH_WE_STRT_D DATE,
        FNCL_RPRT_MNTH_WE_END_D DATE,
        FNCL_QRTR_FRST_DAY_D DATE,
        FNCL_QRTR_LAST_DAY_D DATE,
        FNCL_HFYR_FRST_DAY_D DATE,
        FNCL_HFYR_LAST_DAY_D DATE
        )
        COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
        ;

/* <sc-view> PVTECH.MAP_SAP_INVL_PDCT </sc-view> */
        CREATE OR REPLACE VIEW PVTECH.MAP_SAP_INVL_PDCT
        (
                PDCT,
                PDCT_C,
                EFFT_D,
                EXPY_D
        )
        COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
        AS
        SELECT
        PDCT,
        PDCT_C,
        EFFT_D,
        EXPY_D
  FROM
        STAR_CAD_PROD_DATA.MAP_SAP_INVL_PDCT
        ;

/* <sc-table> STAR_CAD_PROD_DATA.MAP_SAP_INVL_PDCT </sc-table> */
        --** SSC-FDM-TD0024 - SET TABLE FUNCTIONALITY NOT SUPPORTED. TABLE MIGHT HAVE DUPLICATE ROWS **
        CREATE OR REPLACE TABLE STAR_CAD_PROD_DATA.MAP_SAP_INVL_PDCT
        (
        PDCT CHAR(6) NOT NULL COMMENT 'PRODUCT',
        PDCT_C CHAR(4) NOT NULL COMMENT 'PRODUCT CODE',
        EFFT_D DATE NOT NULL COMMENT 'Effective Date',
        EXPY_D DATE NOT NULL DEFAULT '9999-12-31' :: DATE COMMENT 'Expiry Date'
        )
        COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
        ;

/* <sc-view> PVCBODS.ACCT_MSTR_CYT_DATA </sc-view> */
        CREATE OR REPLACE VIEW PVCBODS.ACCT_MSTR_CYT_DATA
        (
        	ACCT,
        	BUSN_PTNR_NUMB,
        	SRCE_SYST_ISAC_CODE,
        	VALD_FROM,
        	VALD_TO,
        	USER_IN_CYT_CALC,
        	CYT_DOCU_QOTE,
        	RSDT_STUS,
        	UPDT_MODE,
        	LOAD_S,
        	PROS_KEY_EFFT_I,
        	ROW_SECU_ACCS_C,
        	RECD_CNT,
        	LAST_UPDT_ON,
        	BDT_OBJC_APPT,
        	CNCT_PART_ID,
        	OBJC_FUNC,
        	PROS_TIME_STMP,
        	LAST_CHNG_BY,
        	BUSN_TRAN_CATG_FOR_THE_CHNG,
        	PDCG,
        	CUST_ENHC_2_ID,
        	TIME_STMP_SHRT_FORM,
        	VALD_FROM_DTTS,
        	VALD_TO_DTTS
        )
        COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
        AS
        SELECT
        ACCT,
        BUSN_PTNR_NUMB,
        SRCE_SYST_ISAC_CODE,
        VALD_FROM,
        VALD_TO,
        USER_IN_CYT_CALC,
        CYT_DOCU_QOTE,
        RSDT_STUS,
        UPDT_MODE,
        LOAD_S,
        PROS_KEY_EFFT_I,
        ROW_SECU_ACCS_C,
        RECD_CNT,
        LAST_UPDT_ON,
        BDT_OBJC_APPT,
        CNCT_PART_ID,
        OBJC_FUNC,
        PROS_TIME_STMP,
        LAST_CHNG_BY,
        BUSN_TRAN_CATG_FOR_THE_CHNG,
        PDCG,
        CUST_ENHC_2_ID,
        TIME_STMP_SHRT_FORM,
        VALD_FROM_DTTS,
        VALD_TO_DTTS
         FROM
        PDCBODS.ACCT_MSTR_CYT_DATA

       WHERE
       (
       /* Start - RLS */
        COALESCE(ROW_SECU_ACCS_C,0) = 0 OR GETBIT( (
		SELECT
			ROW_SECU_PRFL_C
	FROM
			PVSECURITY.ROW_LEVL_SECU_USER_PRFL
	WHERE
			UPPER(RTRIM( USERNAME)) = UPPER(RTRIM(CURRENT_USER))
       ),ROW_SECU_ACCS_C
       ) = 1
       /* End - RLS */
       );

/* <sc-table> PDCBODS.ACCT_MSTR_CYT_DATA </sc-table> */
			--** SSC-FDM-TD0024 - SET TABLE FUNCTIONALITY NOT SUPPORTED. TABLE MIGHT HAVE DUPLICATE ROWS **
			CREATE OR REPLACE TABLE PDCBODS.ACCT_MSTR_CYT_DATA
			(
        ACCT VARCHAR(32) NOT NULL COMMENT 'Account',
        BUSN_PTNR_NUMB VARCHAR(10) NOT NULL COMMENT 'Business Partner Number',
        SRCE_SYST_ISAC_CODE CHAR(6) NOT NULL COMMENT 'Source System Instance Code',
        VALD_FROM DATE NOT NULL COMMENT 'Valid From',
        VALD_TO DATE COMMENT 'Valid To',
        USER_IN_CYT_CALC CHAR(1) COMMENT 'Usage In Cyt Calculation',
        CYT_DOCU_QOTE CHAR(1) COMMENT 'Cyt Document Quotation',
        RSDT_STUS CHAR(1) COMMENT 'Residential Status',
        UPDT_MODE CHAR(1) COMMENT 'Update Mode',
        LOAD_S TIMESTAMP(6) COMMENT 'Load Timestamp', --FORMAT 'YYYY-MM-DDHH:MI:SSDS(6)' - FORMAT IN TABLE PDCBODS.ACCT_MSTR_CYT_DATA NOT SUPPORTED 
        PROS_KEY_EFFT_I INTEGER NOT NULL COMMENT 'Process Key Effective Identifier',
        ROW_SECU_ACCS_C INTEGER COMMENT 'Row Security Access Code',
        RECD_CNT DECIMAL(20,0) DEFAULT NULL COMMENT 'Record Count',
        LAST_UPDT_ON DATE DEFAULT NULL COMMENT 'Last Updated On',
        BDT_OBJC_APPT CHAR(4) DEFAULT NULL COMMENT 'Bdt Object Application',
        CNCT_PART_ID VARCHAR(32) DEFAULT NULL COMMENT 'Contract Part Id' ,
        OBJC_FUNC CHAR(6) DEFAULT NULL COMMENT 'Object Function',
        PROS_TIME_STMP TIMESTAMP(6) DEFAULT NULL COMMENT 'Processing Time Stamp' ,
        LAST_CHNG_BY VARCHAR(12) DEFAULT NULL COMMENT 'Last Changed By' ,
        BUSN_TRAN_CATG_FOR_THE_CHNG CHAR(6) DEFAULT NULL COMMENT 'Business Transaction Category For The Change',
        PDCG CHAR(4) DEFAULT NULL COMMENT 'Product Category',
        CUST_ENHC_2_ID VARCHAR(32) DEFAULT NULL COMMENT 'Customer Enhancement 2 Id' ,
        TIME_STMP_SHRT_FORM TIMESTAMP(6) DEFAULT NULL COMMENT 'Time Stamp Short Form' ,
        VALD_FROM_DTTS TIMESTAMP(6) DEFAULT NULL COMMENT 'Valid From Timestamp' ,
        VALD_TO_DTTS TIMESTAMP(6) DEFAULT NULL COMMENT 'Valid To Timestamp' )
			COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
			;

			CREATE OR REPLACE VIEW pvcbods.BUSN_PTNR
			(
				BUSN_PTNR_NUMB,
				SRCE_SYST_ISAC_CODE,
				PATY_I,
				SAP_PATY_I,
				DOB,
				FRST_ACDM_TITL,
				SCND_ACDM_TITL,
				BUSN_PTNR_NAME_AT_BRTH,
				BUSN_PTNR_CATG,
				BUSN_PTNR_CTCT_PMIS,
				BUSN_PTNR_NUMB_IN_EXTL_SYST,
				BUSN_PTNR_FRST_NAME,
				BUSN_PTNR_GRUP,
				BUSN_PTNR_NAME_1_GRUP,
				BUSN_PTNR_NAME_2_GRUP,
				BUSN_PTNR_GRUP_TYPE,
				BUSN_PTNR_SRNM,
				BUSN_PTNR_MRST,
				BUSN_PTNR_MDLE_NAME,
				BUSN_PTNR_SCND_SRNM,
				PTNR_TYPE,
				CHNG_BY,
				LAST_CHNG_AT,
				LAST_CHNG_ON,
				NAME_OF_PERS_WHO_CRAT_THE_OBJC,
				RECD_CRAT_DATE,
				TIME_CRAT,
				SLCT_GNDR_MALE,
				SLCT_GNDR_FEM,
				SLCT_GNDR_UNKN,
				INDC,
				LEGL_STUS_OF_ORGN,
				BUSN_PTNR_ORGN_NAME_1,
				BUSN_PTNR_ORGN_NAME_2,
				BUSN_PTNR_ORGN_NAME_3,
				BUSN_PTNR_ORGN_NAME_4,
				NATY,
				NAME_SUPT,
				FORM_OF_ADRS_TITL,
				CNTY_FOR_NAME_FRMT_RULE,
				SCND_NAME_PREF,
				DATE_ORGN_FOUN,
				BUSN_PTNR_CORR_LANG,
				BUSN_PTNR_LANG,
				NAME_FRMT,
				DATE_OF_DETH,
				OCCP,
				UPDT_MODE,
				LOAD_S,
				PROS_KEY_EFFT_I,
				ROW_SECU_ACCS_C,
				RECD_CNT,
				ADRS_NUMB,
				BUSN_PTNR_BRTH_PLAC,
				SRCH_TERM_1,
				SRCH_TERM_2,
				CNTY_OF_ORIG,
				CNTY_OF_TAX,
				NAME_OF_EMPR_OF_NATU_PERS,
				MDLE_INLS,
				LENT_FOR_BUSN_PTNR,
				CLTL_OBJC_LIQD_DATE,
				INTL_LOCN_NUMB_PART_1,
				INTL_LOCN_NUMB_PART_2,
				INTL_LOCN_NUMB_PART_3,
				SRCH_HELP_1_NAME_1_LAST_NAME,
				SRCH_HELP_2_NAME_2_FRST_NAME,
				NAME,
				NAME_PART_2,
				BUSN_PTNR_IS_A_NATU_PERS_INDA,
				NCKN,
				GuID_OF_BUSN_PTNR,
				PERS,
				ORDR_PATY_PERS_NUMB,
				NAME_SUPT_PREF_1,
				PRNT_MODE,
				DATA_SRCE,
				PLAN_CHNG_DOCU_FOR_PTNR_CONV,
				SALU,
				VALD_FROM_UTC_DTTS,
				VALD_TO_UTC_DTTS,
				BUSN_PTNR_BLOK,
				BUSN_PTNR_DELT_FLAG,
				AUTN_GRUP,
				UPD_LOAD_S,
				UPD_PROS_KEY_EFFT_I,
				IS_CURR_IND
			)
			COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
			AS
			SELECT
        BUSN_PTNR_NUMB,
        SRCE_SYST_ISAC_CODE,
        PATY_I,
        SAP_PATY_I,
        DOB,
        '***HIDDEN (FRST_ACDM_TITL)***',
        '***HIDDEN (SCND_ACDM_TITL)***',
        '***HIDDEN (BUSN_PTNR_NAME_AT_BRTH)***',
        BUSN_PTNR_CATG,
        BUSN_PTNR_CTCT_PMIS,
        BUSN_PTNR_NUMB_IN_EXTL_SYST,
        '***HIDDEN (BUSN_PTNR_FRST_NAME)***',
        BUSN_PTNR_GRUP,
        '***HIDDEN (BUSN_PTNR_NAME_1_GRUP)***',
        '***HIDDEN (BUSN_PTNR_NAME_2_GRUP)***',
        BUSN_PTNR_GRUP_TYPE,
        '***HIDDEN (BUSN_PTNR_SRNM)***',
        BUSN_PTNR_MRST,
        '***HIDDEN (BUSN_PTNR_MDLE_NAME)***',
        '***HIDDEN (BUSN_PTNR_SCND_SRNM)***',
        PTNR_TYPE,
        CHNG_BY,
        LAST_CHNG_AT,
        LAST_CHNG_ON,
        NAME_OF_PERS_WHO_CRAT_THE_OBJC,
        RECD_CRAT_DATE,
        TIME_CRAT,
        SLCT_GNDR_MALE,
        SLCT_GNDR_FEM,
        SLCT_GNDR_UNKN,
        INDC,
        LEGL_STUS_OF_ORGN,
        '***HIDDEN (BUSN_PTNR_ORGN_NAME_1)***',
        '***HIDDEN (BUSN_PTNR_ORGN_NAME_2)***',
        '***HIDDEN (BUSN_PTNR_ORGN_NAME_3)***',
        '***HIDDEN (BUSN_PTNR_ORGN_NAME_4)***',
        NATY,
        NAME_SUPT,
        FORM_OF_ADRS_TITL,
        CNTY_FOR_NAME_FRMT_RULE,
        SCND_NAME_PREF,
        DATE_ORGN_FOUN,
        BUSN_PTNR_CORR_LANG,
        BUSN_PTNR_LANG,
        NAME_FRMT,
        DATE_OF_DETH,
        OCCP,
        UPDT_MODE,
        LOAD_S,
        PROS_KEY_EFFT_I,
        ROW_SECU_ACCS_C,
        RECD_CNT,
        ADRS_NUMB,
        BUSN_PTNR_BRTH_PLAC,
        SRCH_TERM_1,
        SRCH_TERM_2,
        CNTY_OF_ORIG,
        CNTY_OF_TAX,
        NAME_OF_EMPR_OF_NATU_PERS,
        MDLE_INLS,
        LENT_FOR_BUSN_PTNR,
        CLTL_OBJC_LIQD_DATE,
        INTL_LOCN_NUMB_PART_1,
        INTL_LOCN_NUMB_PART_2,
        INTL_LOCN_NUMB_PART_3,
           '***HIDDEN (SRCH_HELP_1_NAME_1_LAST_NAME)***',
        '***HIDDEN (SRCH_HELP_2_NAME_2_FRST_NAME)***',
           '***HIDDEN (NAME)***',
        '***HIDDEN (NAME_PART_2)***',
        BUSN_PTNR_IS_A_NATU_PERS_INDA,
        '***HIDDEN (NCKN)***',
        GuID_OF_BUSN_PTNR,
        PERS,
        ORDR_PATY_PERS_NUMB,
        NAME_SUPT_PREF_1,
        PRNT_MODE,
        DATA_SRCE,
        PLAN_CHNG_DOCU_FOR_PTNR_CONV,
        SALU,
        VALD_FROM_UTC_DTTS,
        VALD_TO_UTC_DTTS,
        BUSN_PTNR_BLOK,
        BUSN_PTNR_DELT_FLAG,
        AUTN_GRUP,
        UPD_LOAD_S,
        UPD_PROS_KEY_EFFT_I,
        IS_CURR_IND
         FROM
        PDCBODS.BUSN_PTNR

       WHERE
       (
       /* Start - RLS */
        COALESCE(ROW_SECU_ACCS_C,0) = 0 OR GETBIT( (
		SELECT
			ROW_SECU_PRFL_C
	FROM
			PVSECURITY.ROW_LEVL_SECU_USER_PRFL
	WHERE
			UPPER(RTRIM( USERNAME)) = UPPER(RTRIM(CURRENT_USER))
       ),ROW_SECU_ACCS_C
       ) = 1
       /* End - RLS */
       );

/* <sc-table> PDCBODS.BUSN_PTNR </sc-table> */
			CREATE OR REPLACE TABLE PDCBODS.BUSN_PTNR
			(
        BUSN_PTNR_NUMB VARCHAR(10) NOT NULL COMMENT 'Business Partner Number',
        SRCE_SYST_ISAC_CODE CHAR(6) NOT NULL COMMENT 'Source System Instance Code',
        PATY_I VARCHAR(255) NOT NULL COMMENT 'Party Identifier',
        SAP_PATY_I VARCHAR(255) NOT NULL COMMENT 'SAP PATY IDENTIFIER',
        DOB DATE COMMENT 'Date of Birth',
        FRST_ACDM_TITL CHAR(4) COMMENT 'First Academic Title',
        SCND_ACDM_TITL CHAR(4) COMMENT 'Second Academic Title',
        BUSN_PTNR_NAME_AT_BRTH VARCHAR(40) COMMENT 'Business Partner Name At Birth',
        BUSN_PTNR_CATG CHAR(1) COMMENT 'Business Partner Category',
        BUSN_PTNR_CTCT_PMIS CHAR(1) COMMENT 'Business Partner Contact Permission',
        BUSN_PTNR_NUMB_IN_EXTL_SYST VARCHAR(20) COMMENT 'Business Partner Number In External System',
        BUSN_PTNR_FRST_NAME VARCHAR(40) COMMENT 'Business Partner First Name',
        BUSN_PTNR_GRUP CHAR(4) COMMENT 'Business Partner Grouping',
        BUSN_PTNR_NAME_1_GRUP VARCHAR(40) COMMENT 'Business Partner Name 1 Group',
        BUSN_PTNR_NAME_2_GRUP VARCHAR(40) COMMENT 'Business Partner Name 2 Group',
        BUSN_PTNR_GRUP_TYPE CHAR(4) COMMENT 'Business Partner Group Type',
        BUSN_PTNR_SRNM VARCHAR(40) COMMENT 'Business Partner Surname',
        BUSN_PTNR_MRST CHAR(1) COMMENT 'Business Partner Marital Status',
        BUSN_PTNR_MDLE_NAME VARCHAR(40) COMMENT 'Business Partner Middle Name',
        BUSN_PTNR_SCND_SRNM VARCHAR(40) COMMENT 'Business Partner Second Surname',
        PTNR_TYPE CHAR(4) COMMENT 'Partner Type',
        CHNG_BY VARCHAR(12) COMMENT 'Changed by',
        LAST_CHNG_AT TIME(6) COMMENT 'Last Change At', --FORMAT 'HH:MI:SSDS(6)' FORMAT IN TABLE PDCBODS.BUSN_PTNR NOT SUPPORTED 
        LAST_CHNG_ON DATE COMMENT 'Last changed on',
        NAME_OF_PERS_WHO_CRAT_THE_OBJC VARCHAR(12) COMMENT 'Name of person who created the object',
        RECD_CRAT_DATE DATE COMMENT 'Record Created Date',
        TIME_CRAT TIME(6) COMMENT 'Time Created',
        SLCT_GNDR_MALE CHAR(1) COMMENT 'Selection Gender Male',
        SLCT_GNDR_FEM CHAR(1) COMMENT 'Selection Gender Female',
        SLCT_GNDR_UNKN CHAR(1) COMMENT 'Selection Gender Unknown',
        INDC VARCHAR(10) COMMENT 'Industry code',
        LEGL_STUS_OF_ORGN CHAR(2) COMMENT 'Legal Status Of Organisation',
        BUSN_PTNR_ORGN_NAME_1 VARCHAR(40) COMMENT 'Business Partner Organisation Name 1',
        BUSN_PTNR_ORGN_NAME_2 VARCHAR(40) COMMENT 'Business Partner Organisation Name 2',
        BUSN_PTNR_ORGN_NAME_3 VARCHAR(40) COMMENT 'Business Partner Organisation Name 3',
        BUSN_PTNR_ORGN_NAME_4 VARCHAR(40) COMMENT 'Business Partner Organisation Name 4',
        NATY CHAR(3) COMMENT 'Nationality',
        NAME_SUPT CHAR(4) COMMENT 'Name Supplement',
        FORM_OF_ADRS_TITL VARCHAR(15) COMMENT 'Form of Address Title',
        CNTY_FOR_NAME_FRMT_RULE CHAR(3) COMMENT 'Country for name format rule',
        SCND_NAME_PREF CHAR(4) COMMENT 'Second name prefix',
        DATE_ORGN_FOUN DATE COMMENT 'Date organisation found',
        BUSN_PTNR_CORR_LANG CHAR(2) COMMENT 'Business Partner Correspondence Language',
        BUSN_PTNR_LANG CHAR(2) COMMENT 'Business Partner Language',
        NAME_FRMT CHAR(2) COMMENT 'Name format',
        DATE_OF_DETH DATE COMMENT 'Date of Death',
        OCCP CHAR(4) COMMENT 'Occupation',
        UPDT_MODE CHAR(1) COMMENT 'Update Mode',
        LOAD_S TIMESTAMP(6) COMMENT 'Load Timestamp',
        PROS_KEY_EFFT_I INTEGER NOT NULL COMMENT 'Process Key Effective Identifier',
        ROW_SECU_ACCS_C INTEGER NOT NULL DEFAULT 0 COMMENT 'Row Security Access Code',
        RECD_CNT DECIMAL(18,0) DEFAULT NULL COMMENT 'Record Count',
        ADRS_NUMB VARCHAR(10) DEFAULT NULL COMMENT 'Address Number' ,
        BUSN_PTNR_BRTH_PLAC VARCHAR(40) DEFAULT NULL COMMENT 'Business Partner Birth Place' ,
        SRCH_TERM_1 VARCHAR(20) DEFAULT NULL COMMENT 'Search Term 1' ,
        SRCH_TERM_2 VARCHAR(20) DEFAULT NULL COMMENT 'Search Term 2' ,
        CNTY_OF_ORIG CHAR(3) DEFAULT NULL COMMENT 'Country Of Origin',
        CNTY_OF_TAX CHAR(3) DEFAULT NULL COMMENT 'Country Of Tax',
        NAME_OF_EMPR_OF_NATU_PERS VARCHAR(35) DEFAULT NULL COMMENT 'Name Of Employer Of Natural Person' ,
        MDLE_INLS VARCHAR(10) DEFAULT NULL COMMENT 'Middle Initials' ,
        LENT_FOR_BUSN_PTNR CHAR(2) DEFAULT NULL COMMENT 'Legal Entity For Business Partner',
        CLTL_OBJC_LIQD_DATE DATE DEFAULT NULL COMMENT 'Collateral Object Liquidation Date',
        INTL_LOCN_NUMB_PART_1 DECIMAL(7,0) DEFAULT NULL COMMENT 'International Location Number Part 1',
        INTL_LOCN_NUMB_PART_2 DECIMAL(7,0) DEFAULT NULL COMMENT 'International Location Number Part 2',
        INTL_LOCN_NUMB_PART_3 DECIMAL(7,0) DEFAULT NULL COMMENT 'International Location Number Part 3',
        SRCH_HELP_1_NAME_1_LAST_NAME VARCHAR(35) DEFAULT NULL COMMENT 'Search Help 1 Name 1 Last Name' ,
        SRCH_HELP_2_NAME_2_FRST_NAME VARCHAR(35) DEFAULT NULL COMMENT 'Search Help 2 Name 2 First Name' ,
        NAME VARCHAR(60) DEFAULT NULL COMMENT 'Name' ,
        NAME_PART_2 VARCHAR(20) DEFAULT NULL COMMENT 'Name Part 2' ,
        BUSN_PTNR_IS_A_NATU_PERS_INDA CHAR(1) DEFAULT NULL COMMENT 'Business Partner Is A Natural Person Indicator',
        NCKN VARCHAR(10) DEFAULT NULL COMMENT 'Nickname' ,
        GuID_OF_BUSN_PTNR VARCHAR(32) DEFAULT NULL COMMENT 'Guid Of Business Partner' ,
        PERS DECIMAL(8,0) DEFAULT NULL COMMENT 'Person',
        ORDR_PATY_PERS_NUMB VARCHAR(10) DEFAULT NULL COMMENT 'Ordering Party Person Number' ,
        NAME_SUPT_PREF_1 CHAR(4) DEFAULT NULL COMMENT 'Name Supplement Prefix 1',
        PRNT_MODE CHAR(1) DEFAULT NULL COMMENT 'Print Mode',
        DATA_SRCE CHAR(4) DEFAULT NULL COMMENT 'Data Source',
        PLAN_CHNG_DOCU_FOR_PTNR_CONV CHAR(1) DEFAULT NULL COMMENT 'Planned Change Documents For Partner Convert',
        SALU VARCHAR(50) DEFAULT NULL COMMENT 'Salutation' ,
        VALD_FROM_UTC_DTTS TIMESTAMP(6) DEFAULT NULL COMMENT 'Valid From UTC Timestamp' ,
        VALD_TO_UTC_DTTS TIMESTAMP(6) DEFAULT NULL COMMENT 'Valid To UTC Timestamp' ,
        BUSN_PTNR_BLOK CHAR(1) DEFAULT NULL COMMENT 'Business Partner Block',
        BUSN_PTNR_DELT_FLAG CHAR(1) DEFAULT NULL COMMENT 'Business Partner Delete Flag',
        UPD_LOAD_S TIMESTAMP(6) COMMENT 'Updating Load Timestamp',
        UPD_PROS_KEY_EFFT_I INTEGER COMMENT 'Updating Process Key Effective Identifier',
        IS_CURR_IND BYTEINT NOT NULL DEFAULT 0 COMMENT 'IS Current Indicator',
        AUTN_GRUP CHAR(4) DEFAULT NULL COMMENT 'Authorisation Group'
			)
			COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
			;

/* <sc-view> PVCBODS.CBA_FNCL_SERV_GL_DATA_CURR </sc-view> */
			CREATE OR REPLACE VIEW PVCBODS.CBA_FNCL_SERV_GL_DATA_CURR
			(
				RECD_CNT,
				INTR_CNCT_ID,
				INTR_OBJC_IDNN,
				SRCE_SYST_ISAC_CODE,
				OBJC_BDT_APPT,
				OBJC_FUNC,
				CHNG_TIME_STMP,
				VALD_FROM,
				ACTL_VALD_END,
				LAST_CHNG_BY,
				BUSN_TRAN_CATG,
				SALE_PDCT,
				DEPT_ID,
				AFLT,
				ALT_PDCT_ID,
				SHRT_FORM_TIME_STMP,
				MIGR_GRUP,
				UPDT_MODE,
				LOAD_S,
				PROS_KEY_EFFT_I,
				ROW_SECU_ACCS_C,
				UPD_LOAD_S,
				UPD_PROS_KEY_EFFT_I,
				IS_CURR_IND
			)
			COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
			AS
			SELECT
        RECD_CNT,
        INTR_CNCT_ID,
        INTR_OBJC_IDNN,
        SRCE_SYST_ISAC_CODE,
        OBJC_BDT_APPT,
        OBJC_FUNC,
        CHNG_TIME_STMP,
        VALD_FROM,
        ACTL_VALD_END,
        LAST_CHNG_BY,
        BUSN_TRAN_CATG,
        SALE_PDCT,
        DEPT_ID,
        AFLT,
        ALT_PDCT_ID,
        SHRT_FORM_TIME_STMP,
        MIGR_GRUP,
        UPDT_MODE,
        LOAD_S,
        PROS_KEY_EFFT_I,
        ROW_SECU_ACCS_C,
        UPD_LOAD_S,
        UPD_PROS_KEY_EFFT_I,
        IS_CURR_IND
         FROM
        PDCBODS.CBA_FNCL_SERV_GL_DATA
         WHERE  IS_CURR_IND = 1  /* Retrieve only the "Current" record per Primary Key */

        AND


       (
       (
       /* Start - RLS */
        COALESCE(ROW_SECU_ACCS_C,0) = 0 OR GETBIT( (
		SELECT
			ROW_SECU_PRFL_C
	FROM
			PVSECURITY.ROW_LEVL_SECU_USER_PRFL
	WHERE
			UPPER(RTRIM( USERNAME)) = UPPER(RTRIM(CURRENT_USER))
       ),ROW_SECU_ACCS_C
       ) = 1
       /* End - RLS */
       )
       );

/* <sc-table> PDCBODS.CBA_FNCL_SERV_GL_DATA </sc-table> */
			CREATE OR REPLACE TABLE PDCBODS.CBA_FNCL_SERV_GL_DATA
			(
        RECD_CNT DECIMAL(20,0) COMMENT 'Record Count',
        INTR_CNCT_ID VARCHAR(32) NOT NULL COMMENT 'Internal Contract Id',
        INTR_OBJC_IDNN VARCHAR(32) NOT NULL COMMENT 'Internal Object Identification',
        SRCE_SYST_ISAC_CODE CHAR(6) NOT NULL COMMENT 'Source System Instance Code',
        OBJC_BDT_APPT CHAR(4) COMMENT 'Object Bdt Application',
        OBJC_FUNC CHAR(6) COMMENT 'Object Function',
        CHNG_TIME_STMP TIMESTAMP(6) COMMENT 'Change Time Stamp',
        VALD_FROM TIMESTAMP(6) COMMENT 'Valid From',
        ACTL_VALD_END TIMESTAMP(6) COMMENT 'Actual Validity End',
        LAST_CHNG_BY VARCHAR(12) COMMENT 'Last Changed By',
        BUSN_TRAN_CATG CHAR(6) COMMENT 'Business Transaction Category',
        SALE_PDCT CHAR(6) COMMENT 'Sales Product',
        DEPT_ID CHAR(6) COMMENT 'Department Id',
        AFLT CHAR(5) COMMENT 'Affiliate',
        ALT_PDCT_ID CHAR(6) COMMENT 'Alternate Product Id',
        SHRT_FORM_TIME_STMP TIMESTAMP(6) COMMENT 'Short Form Time Stamp',
        MIGR_GRUP CHAR(5) COMMENT 'Migration Group',
        UPDT_MODE CHAR(1) COMMENT 'Update Mode',
        LOAD_S TIMESTAMP(6) COMMENT 'Load Timestamp',
        PROS_KEY_EFFT_I INTEGER NOT NULL COMMENT 'Process Key Effective Identifier',
        ROW_SECU_ACCS_C INTEGER NOT NULL DEFAULT 0 COMMENT 'Row Security Access Code',
        UPD_LOAD_S TIMESTAMP(6) COMMENT 'Update Load Timestamp (Physical Only)',
        UPD_PROS_KEY_EFFT_I INTEGER COMMENT 'Update Process Key Effective ID (Physical Only)',
        IS_CURR_IND BYTEINT NOT NULL COMMENT 'Is Current Indicator'
			)
			COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
			;

/* <sc-view> PVCBODS.MSTR_CNCT_BALN_TRNF_PRTP </sc-view> */
			CREATE OR REPLACE VIEW PVCBODS.MSTR_CNCT_BALN_TRNF_PRTP
			(
				RECD_CNT,
				INTR_CNCT_NUMB_OF_MAIN_CNCT,
				ELEM_OF_A_CNCT_HIER_GRUP,
				SRCE_SYST_ISAC_CODE,
				OBJC_BDT_APPT,
				INTR_OBJC_IDNN,
				OBJC_FUNC,
				TREE_TYPE,
				CNCT_CATG,
				MAIN_CNCT_CATG,
				VALD_FROM,
				ACTL_VALD_END,
				MEMB_BDT_APPT,
				OBJC_FIX_PRTP_MAIN_CNCT,
				HIER_ID,
				BALN_TRNF_ROLE,
				TSHD_AMT,
				CNCY,
				FUND_ACCT_RANK,
				MAX_PERC_RATE,
				BALN_TRNF_BALN_DETR_BSIS,
				SHRT_FORM_TIME_STMP,
				UPDT_MODE,
				LOAD_S,
				PROS_KEY_EFFT_I,
				ROW_SECU_ACCS_C,
				UPD_LOAD_S,
				UPD_PROS_KEY_EFFT_I,
				IS_CURR_IND
			)
			COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
			AS
			SELECT
        RECD_CNT,
        INTR_CNCT_NUMB_OF_MAIN_CNCT,
        ELEM_OF_A_CNCT_HIER_GRUP,
        SRCE_SYST_ISAC_CODE,
        OBJC_BDT_APPT,
        INTR_OBJC_IDNN,
        OBJC_FUNC,
        TREE_TYPE,
        CNCT_CATG,
        MAIN_CNCT_CATG,
        VALD_FROM,
        ACTL_VALD_END,
        MEMB_BDT_APPT,
        OBJC_FIX_PRTP_MAIN_CNCT,
        HIER_ID,
        BALN_TRNF_ROLE,
        TSHD_AMT,
        CNCY,
        FUND_ACCT_RANK,
        MAX_PERC_RATE,
        BALN_TRNF_BALN_DETR_BSIS,
        SHRT_FORM_TIME_STMP,
        UPDT_MODE,
        LOAD_S,
        PROS_KEY_EFFT_I,
        ROW_SECU_ACCS_C,
        UPD_LOAD_S,
        UPD_PROS_KEY_EFFT_I,
        IS_CURR_IND
         FROM
        PDCBODS.MSTR_CNCT_BALN_TRNF_PRTP

       WHERE
       (
       /* Start - RLS */
        COALESCE(ROW_SECU_ACCS_C,0) = 0 OR GETBIT( (
		SELECT
			ROW_SECU_PRFL_C
	FROM
			PVSECURITY.ROW_LEVL_SECU_USER_PRFL
	WHERE
			UPPER(RTRIM( USERNAME)) = UPPER(RTRIM(CURRENT_USER))
       ),ROW_SECU_ACCS_C
       ) = 1
       /* End - RLS */
       );

/* <sc-table> PDCBODS.MSTR_CNCT_BALN_TRNF_PRTP </sc-table> */
			CREATE OR REPLACE TABLE PDCBODS.MSTR_CNCT_BALN_TRNF_PRTP
			(
        RECD_CNT DECIMAL(20,0) COMMENT 'Record Count',
        INTR_CNCT_NUMB_OF_MAIN_CNCT VARCHAR(32) NOT NULL COMMENT 'Internal Contract Number Of Main Contract',
        ELEM_OF_A_CNCT_HIER_GRUP VARCHAR(32) NOT NULL COMMENT 'Element Of A Contract Hierarchy Group',
        SRCE_SYST_ISAC_CODE CHAR(6) NOT NULL COMMENT 'Source System Instance Code',
        OBJC_BDT_APPT CHAR(4) COMMENT 'Object Bdt Application',
        INTR_OBJC_IDNN VARCHAR(32) COMMENT 'Internal Object Identification',
        OBJC_FUNC CHAR(6) COMMENT 'Object Function',
        TREE_TYPE CHAR(1) COMMENT 'Tree Type',
        CNCT_CATG CHAR(4) COMMENT 'Contract Category',
        MAIN_CNCT_CATG CHAR(4) COMMENT 'Main Contract Category',
        VALD_FROM TIMESTAMP(6) COMMENT 'Valid From',
        ACTL_VALD_END TIMESTAMP(6) COMMENT 'Actual Validity End',
        MEMB_BDT_APPT CHAR(4) COMMENT 'Member Bdt Application',
        OBJC_FIX_PRTP_MAIN_CNCT CHAR(1) COMMENT 'Object Fixed Participant Main Contract',
        HIER_ID VARCHAR(32) COMMENT 'Hierarchy Id',
        BALN_TRNF_ROLE CHAR(1) COMMENT 'Balance Transfer Role',
        TSHD_AMT DECIMAL(23,5) COMMENT 'Threshold Amount',
        CNCY CHAR(3) COMMENT 'Currency',
        FUND_ACCT_RANK DECIMAL(3,0) COMMENT 'Funding Account Ranking',
        MAX_PERC_RATE DECIMAL(4,0) COMMENT 'Maximum Percentage Rate',
        BALN_TRNF_BALN_DETR_BSIS CHAR(4) COMMENT 'Balance Transfer Balance Determination Basis',
        SHRT_FORM_TIME_STMP TIMESTAMP(6) COMMENT 'Short Form Time Stamp',
        UPDT_MODE CHAR(1) COMMENT 'Update Mode',
        LOAD_S TIMESTAMP(6) COMMENT 'Load Timestamp',
        PROS_KEY_EFFT_I INTEGER NOT NULL COMMENT 'Process Key Effective Identifier',
        ROW_SECU_ACCS_C INTEGER NOT NULL DEFAULT 0 COMMENT 'Row Security Access Code',
        UPD_LOAD_S TIMESTAMP(6) COMMENT 'Update Load Timestamp (Physical Only)',
        UPD_PROS_KEY_EFFT_I INTEGER COMMENT 'Update Process Key Effective Identifier',
        IS_CURR_IND BYTEINT NOT NULL COMMENT 'Is Current Indicator'
			)
			COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
			;

/* <sc-view> PVCBODS.MSTR_CNCT_MSTR_DATA_GENL </sc-view> */
			CREATE OR REPLACE VIEW PVCBODS.MSTR_CNCT_MSTR_DATA_GENL
			(
			 RECD_CNT,
			 INTR_CNCT_NUMB_OF_MAIN_CNCT,
			 SRCE_SYST_ISAC_CODE,
			 ACCT_I,
			 OBJC_BDT_APPT,
			 CHNG_TIME_STMP,
			 VALD_STRT,
			 ACTL_VALD_END,
			 LAST_CHNG_BY,
			 BUSN_TRAN_CATG_FOR_THE_CHNG,
			 MSTR_CNCT_NUMB,
			 SHRT_FORM_TIME_STMP,
			 UPDT_MODE,
			 LOAD_S,
			 PROS_KEY_EFFT_I,
			 ROW_SECU_ACCS_C,
			 ORGN_UNIT,
			 CNCT_CLAR_1,
			 CNCT_CLAR_2,
			 CNCT_CLAR_3,
			 BANK_POST_AREA
			)
			COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
			AS
			SELECT
        RECD_CNT,
        INTR_CNCT_NUMB_OF_MAIN_CNCT,
        SRCE_SYST_ISAC_CODE,
        ACCT_I,
        OBJC_BDT_APPT,
        CHNG_TIME_STMP,
        VALD_STRT,
        ACTL_VALD_END,
        LAST_CHNG_BY,
        BUSN_TRAN_CATG_FOR_THE_CHNG,
        MSTR_CNCT_NUMB,
        SHRT_FORM_TIME_STMP,
        UPDT_MODE,
        LOAD_S,
        PROS_KEY_EFFT_I,
        ROW_SECU_ACCS_C,
        ORGN_UNIT,
        CNCT_CLAR_1,
        CNCT_CLAR_2,
        CNCT_CLAR_3,
        BANK_POST_AREA
         FROM
        PDCBODS.MSTR_CNCT_MSTR_DATA_GENL

       WHERE
       (
      /* Start - RLS */
        COALESCE(ROW_SECU_ACCS_C,0) = 0 OR GETBIT( (
		SELECT
			ROW_SECU_PRFL_C
	FROM
			PVSECURITY.ROW_LEVL_SECU_USER_PRFL
	WHERE
			UPPER(RTRIM( USERNAME)) = UPPER(RTRIM(CURRENT_USER))
      ),ROW_SECU_ACCS_C
      ) = 1
      /* End - RLS */
      )

			-- $LastChangedBy: pavithra$
			-- $LastChangedDate: $
			-- $LastChangedRevision: 6006$
			;

/* <sc-table> PDCBODS.MSTR_CNCT_MSTR_DATA_GENL </sc-table> */
			--** SSC-FDM-TD0024 - SET TABLE FUNCTIONALITY NOT SUPPORTED. TABLE MIGHT HAVE DUPLICATE ROWS **
			CREATE OR REPLACE TABLE PDCBODS.MSTR_CNCT_MSTR_DATA_GENL
			(
        RECD_CNT DECIMAL(20,0) COMMENT 'Record Count',
        INTR_CNCT_NUMB_OF_MAIN_CNCT VARCHAR(32) NOT NULL COMMENT 'Internal Contract Number Of Main Contract',
        SRCE_SYST_ISAC_CODE CHAR(6) NOT NULL COMMENT 'Source System Instance Code',
        ACCT_I VARCHAR(255) COMMENT 'Account Identifier',
        OBJC_BDT_APPT CHAR(4) COMMENT 'Object Bdt Application',
        CHNG_TIME_STMP TIMESTAMP(6) COMMENT 'Change Time Stamp',
        VALD_STRT TIMESTAMP(6) COMMENT 'Validity Start',
        ACTL_VALD_END TIMESTAMP(6) COMMENT 'Actual Validity End',
        LAST_CHNG_BY VARCHAR(12) COMMENT 'Last Changed By',
        BUSN_TRAN_CATG_FOR_THE_CHNG CHAR(6) COMMENT 'Business Transaction Category For The Change',
        MSTR_CNCT_NUMB VARCHAR(20) COMMENT 'Master Contract Number',
        SHRT_FORM_TIME_STMP TIMESTAMP(6) COMMENT 'Short Form Time Stamp',
        UPDT_MODE CHAR(1) COMMENT 'Update Mode',
        LOAD_S TIMESTAMP(6) COMMENT 'Load Timestamp',
        PROS_KEY_EFFT_I INTEGER NOT NULL COMMENT 'Process Key Effective Identifier',
        ROW_SECU_ACCS_C INTEGER NOT NULL DEFAULT 0 COMMENT 'Row Security Access Code',
        ORGN_UNIT DECIMAL(8,0) COMMENT 'Organization Unit',
        CNCT_CLAR_1 CHAR(2) COMMENT 'Contract Calender 1',
        CNCT_CLAR_2 CHAR(2) COMMENT 'Contract Calender 2',
        CNCT_CLAR_3 CHAR(2) COMMENT 'Contract Calender 3',
        BANK_POST_AREA CHAR(4) COMMENT 'Bank Posting Area'
			)
			COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
			;

/* <sc-view> PVCBODS.MSTR_CNCT_PRXY_ACCT </sc-view> */
			CREATE OR REPLACE VIEW PVCBODS.MSTR_CNCT_PRXY_ACCT
			(
				RECD_CNT,
				ELEM_OF_A_CNCT_HIER_GRUP,
				CNCT_TYPE_KEY,
				SRCE_SYST_ISAC_CODE,
				EXTL_CNCT_PART_1,
				EXTL_CNCT_PART_2,
				EXTL_CNCT_PART_3,
				ACCT_CATG,
				UPDT_MODE,
				LOAD_S,
				PROS_KEY_EFFT_I,
				ROW_SECU_ACCS_C,
				NON_SAP_ACCT_IDNN,
				NON_SAP_ACCT_SALE_PDCT,
				NON_SAP_ACCT_NUMB,
				CIF_PDCT_CODE,
				CIF_SUB_PDCT_CODE,
				APPT_CODE,
				SRCE_SYST,
				CIF_PDCT_IDNN,
				CRIS_PDCT_C
			)
			COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
			AS
			SELECT
        RECD_CNT,
        ELEM_OF_A_CNCT_HIER_GRUP,
        CNCT_TYPE_KEY,
        SRCE_SYST_ISAC_CODE,
        EXTL_CNCT_PART_1,
        EXTL_CNCT_PART_2,
        EXTL_CNCT_PART_3,
        ACCT_CATG,
        UPDT_MODE,
        LOAD_S,
        PROS_KEY_EFFT_I,
        ROW_SECU_ACCS_C,
        NON_SAP_ACCT_IDNN,
        NON_SAP_ACCT_SALE_PDCT,
        NON_SAP_ACCT_NUMB,
        CIF_PDCT_CODE,
        CIF_SUB_PDCT_CODE,
        APPT_CODE,
        SRCE_SYST,
        CIF_PDCT_IDNN,
        CRIS_PDCT_C
         FROM
        PDCBODS.MSTR_CNCT_PRXY_ACCT

       WHERE
       (
       /* Start - RLS */
        COALESCE(ROW_SECU_ACCS_C,0) = 0 OR GETBIT( (
		SELECT
			ROW_SECU_PRFL_C
	FROM
			PVSECURITY.ROW_LEVL_SECU_USER_PRFL
	WHERE
			UPPER(RTRIM( USERNAME)) = UPPER(RTRIM(CURRENT_USER))
       ),ROW_SECU_ACCS_C
       ) = 1
       /* End - RLS */
       );

/* <sc-table> PDCBODS.MSTR_CNCT_PRXY_ACCT </sc-table> */
			--** SSC-FDM-TD0024 - SET TABLE FUNCTIONALITY NOT SUPPORTED. TABLE MIGHT HAVE DUPLICATE ROWS **
			CREATE OR REPLACE TABLE PDCBODS.MSTR_CNCT_PRXY_ACCT
			(
        RECD_CNT DECIMAL(20,0) COMMENT 'Record Count',
        ELEM_OF_A_CNCT_HIER_GRUP VARCHAR(32) NOT NULL COMMENT 'Element of a Contract Hierarchy Group',
        CNCT_TYPE_KEY DECIMAL(2,0) NOT NULL COMMENT 'Contract Type Key',
        SRCE_SYST_ISAC_CODE CHAR(6) NOT NULL COMMENT 'Source System Instance Code',
        EXTL_CNCT_PART_1 VARCHAR(32) COMMENT 'External Contract Part 1',
        EXTL_CNCT_PART_2 VARCHAR(60) COMMENT 'External Contract Part 2',
        EXTL_CNCT_PART_3 VARCHAR(8) COMMENT 'External Contract Part 3',
        ACCT_CATG CHAR(6) COMMENT 'Account Category',
        UPDT_MODE CHAR(1) COMMENT 'Update Mode',
        LOAD_S TIMESTAMP(6) COMMENT 'Load Timestamp',
        PROS_KEY_EFFT_I INTEGER NOT NULL COMMENT 'Process Key Effective Identifier',
        ROW_SECU_ACCS_C INTEGER NOT NULL DEFAULT 0 COMMENT 'Row Security Access Code',
        NON_SAP_ACCT_IDNN VARCHAR(10) DEFAULT NULL COMMENT 'Non Sap Account Identification Code' ,
        NON_SAP_ACCT_SALE_PDCT CHAR(6) DEFAULT NULL COMMENT 'Non Sap Account Sale Product',
        NON_SAP_ACCT_NUMB VARCHAR(32) DEFAULT NULL COMMENT 'Non Sap Account Number' ,
        CIF_PDCT_CODE CHAR(3) DEFAULT NULL COMMENT 'Cif Product Code',
        CIF_SUB_PDCT_CODE CHAR(2) DEFAULT NULL COMMENT 'Cif Sub Product Code',
        APPT_CODE CHAR(2) DEFAULT NULL COMMENT 'APPT_CODE',
        SRCE_SYST CHAR(3) DEFAULT NULL COMMENT 'Source System',
        CIF_PDCT_IDNN CHAR(5) DEFAULT NULL COMMENT 'CIF_PDCT_IDNN',
        CRIS_PDCT_C CHAR(3) DEFAULT NULL COMMENT 'CRIS_PDCT_C'
			)
			COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
			;

/* <sc-view> PVSECURITY.ROW_LEVL_SECU_USER_PRFL </sc-view> */
			---3.0 Create new 1:1 views for the above tables
			CREATE OR REPLACE VIEW PVSECURITY.ROW_LEVL_SECU_USER_PRFL
			(
			      USERNAME,
			      ROW_SECU_PRFL_C,
			      MY_SERVICE_NO,
			      REQ_NO,
			      RITM_NO,
			      SAR_NO,
			      CMMT,
			      UPDT_USERNAME,
			      UPDT_DATE,
			      UPDT_DTTS
			)
			COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
			AS
			SELECT
        USERNAME,
        ROW_SECU_PRFL_C,
        MY_SERVICE_NO,
        REQ_NO,
        RITM_NO,
        SAR_NO,
        CMMT,
        UPDT_USERNAME,
        UPDT_DATE,
        UPDT_DTTS
    FROM
        PDSECURITY.ROW_LEVL_SECU_USER_PRFL
        ;

/* <sc-table> PDSECURITY.ROW_LEVL_SECU_USER_PRFL </sc-table> */
        CREATE OR REPLACE TABLE PDSECURITY.ROW_LEVL_SECU_USER_PRFL (
        USERNAME VARCHAR(128) NOT NULL COMMENT 'User Name',
        ROW_SECU_PRFL_C BINARY(130) NOT NULL COMMENT 'Row Security Profile Code',
        MY_SERVICE_NO VARCHAR(10) COMMENT 'MyServiceNo',
        REQ_NO VARCHAR(12) COMMENT 'REQNo',
        RITM_NO VARCHAR(12) COMMENT 'RITMNo',
        SAR_NO VARCHAR(10) COMMENT 'SARNo',
        CMMT VARCHAR(255) COMMENT 'Comments',
        UPDT_USERNAME VARCHAR(128) NOT NULL COMMENT 'User Name',
        UPDT_DATE DATE NOT NULL COMMENT 'Update Date',
        UPDT_DTTS TIMESTAMP(0) NOT NULL COMMENT 'Update DateTime',
        UNIQUE (USERNAME)
        )
        COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
        ;