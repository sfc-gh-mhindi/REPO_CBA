USE DATABASE NPD_D12_DMN_GDWMIG_IBRG_V;

CREATE SCHEMA IF NOT EXISTS CALBASICS;
CREATE SCHEMA IF NOT EXISTS CALENDARTMP;
CREATE SCHEMA IF NOT EXISTS PVCBODS;
CREATE SCHEMA IF NOT EXISTS PVDATA;
CREATE SCHEMA IF NOT EXISTS PVDSTG;
CREATE SCHEMA IF NOT EXISTS PVPATY;
CREATE SCHEMA IF NOT EXISTS PVSECURITY;
CREATE SCHEMA IF NOT EXISTS PVTECH;
CREATE SCHEMA IF NOT EXISTS Sys_Calendar;
CREATE SCHEMA IF NOT EXISTS SYSLIB;


CREATE OR REPLACE VIEW PVCBODS.ODS_RULE
(
	RULE_CODE,
	RULE_STEP_SEQN,
	PRTY,
	VALD_FROM,
	VALD_TO,
	RULE_DESN,
	RULE_STEP_DESN,
	LKUP1_TEXT,
	LKUP1_NUMB,
	LKUP1_DATE,
	LKUP1_ADD_ATTR,
	LKUP2_TEXT,
	LKUP2_NUMB,
	LKUP2_DATE,
	LKUP2_ADD_ATTR,
	RULE_CMMT,
	UPDT_DTTS,
	CRAT_DTTS
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
AS
SELECT
	
	RULE_CODE,
	RULE_STEP_SEQN,
	PRTY,
	VALD_FROM,
	VALD_TO,
	RULE_DESN,
	RULE_STEP_DESN,
	LKUP1_TEXT,
	LKUP1_NUMB,
	LKUP1_DATE,
	LKUP1_ADD_ATTR,
	LKUP2_TEXT,
	LKUP2_NUMB,
	LKUP2_DATE,
	LKUP2_ADD_ATTR,
	RULE_CMMT,
	UPDT_DTTS,
	CRAT_DTTS
	
	 FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDCBODS.ODS_RULE
	;



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
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
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
	NPD_D12_DMN_GDWMIG_IBRG.PDSECURITY.ROW_LEVL_SECU_USER_PRFL
	;



CREATE OR REPLACE VIEW PVDATA.GDW_EFFT_DATE
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.GDW_EFFT_DATE;



--Create New View

CREATE OR REPLACE VIEW PVTECH.INT_GRUP_DEPT
             (
                     INT_GRUP_I,
                     DEPT_I,
                     DEPT_ROLE_C,
                     SRCE_SYST_C,
                     VALD_FROM_D,
                     VALD_TO_D,
                     EFFT_D,
                     EXPY_D,
                     PROS_KEY_EFFT_I,
                     PROS_KEY_EXPY_I,
                     ROW_SECU_ACCS_C
             )
             COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
             AS
             SELECT
	
	INT_GRUP_I,
	DEPT_I,
	DEPT_ROLE_C,
	SRCE_SYST_C,
	VALD_FROM_D,
	VALD_TO_D,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	ROW_SECU_ACCS_C
	
FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.INT_GRUP_DEPT
WHERE
(
/* Start - RLS */
	COALESCE("row_secu_accs_c",0) = 0 OR GETBIT( (
	SELECT
             try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
FROM
             NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
WHERE
             UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
),"row_secu_accs_c"
) = 1
/* End - RLS */
);

CREATE OR REPLACE VIEW PVDATA.INT_GRUP_DEPT_CURR
	(
	        INT_GRUP_I,
	        DEPT_I,
	        DEPT_ROLE_C,
	        SRCE_SYST_C,
	        VALD_FROM_D,
	        VALD_TO_D,
	        EFFT_D,
	        EXPY_D,
	        GDW_EFFT_D
	)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	int_grup_i,
	dept_i,
	dept_role_c,
	srce_syst_c,
	vald_from_d,
	vald_to_d,
	efft_d,
	expy_d,
(
	SELECT
             "gdw_efft_d" FROM
             NPD_D12_DMN_GDWMIG_IBRG_V.PVDATA.GDW_EFFT_DATE
	) AS "gdw_efft_d"
FROM
	PVTECH.INT_GRUP_DEPT
WHERE
	"gdw_efft_d" BETWEEN efft_d AND expy_d;


    
CREATE OR REPLACE VIEW PVTECH.PATY_INT_GRUP
	(
	        INT_GRUP_I,
		 PATY_I,
		 EFFT_D,
		 EXPY_D,
		 SRCE_SYST_C,
		 PROS_KEY_EFFT_I,
		 PROS_KEY_EXPY_I,
		 EROR_SEQN_I,
		 PRIM_CLNT_F,
		 REL_I,
		 SRCE_SYST_PATY_INT_GRUP_I,
		 ORIG_SRCE_SYST_PATY_TYPE_C,
		 ORIG_SRCE_SYST_PATY_I,
		 REL_C,
		 VALD_FROM_D,
		 VALD_TO_D,
		 ROW_SECU_ACCS_C,
		 PRIM_CLNT_SLCT_C
	)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	
	INT_GRUP_I,
	PATY_I,
	EFFT_D,
	EXPY_D,
	SRCE_SYST_C,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	PRIM_CLNT_F,
	REL_I,
	SRCE_SYST_PATY_INT_GRUP_I,
	ORIG_SRCE_SYST_PATY_TYPE_C,
	ORIG_SRCE_SYST_PATY_I,
	REL_C,
	VALD_FROM_D,
	VALD_TO_D,
	ROW_SECU_ACCS_C,
	PRIM_CLNT_SLCT_C
	
FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.PATY_INT_GRUP
WHERE (
-- /* Start - RLS */
-- 	COALESCE(row_secu_accs_c,0) = 0 OR GETBIT( (
-- 	SELECT
--              row_secu_prfl_c
-- FROM
--              NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
-- WHERE
--              UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
-- ),row_secu_accs_c
-- ) = 1
-- /* End - RLS */

/* Start - RLS */
	COALESCE("row_secu_accs_c",0) = 0 OR GETBIT( (
	SELECT
             try_to_number(row_secu_prfl_c::varchar)  --Need to double check this logic
FROM
             NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
WHERE
             UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
),"row_secu_accs_c"
) = 1
/* End - RLS */
);


CREATE OR REPLACE VIEW PVDATA.PATY_INT_GRUP_CURR
	(
	INT_GRUP_I,
	 PATY_I,
	 EFFT_D,
	 EXPY_D,
	 SRCE_SYST_C,
	 PRIM_CLNT_F,
	 REL_I,
	 SRCE_SYST_PATY_INT_GRUP_I,
	 ORIG_SRCE_SYST_PATY_TYPE_C,
	 ORIG_SRCE_SYST_PATY_I,
	 REL_C,
	 VALD_FROM_D,
	 VALD_TO_D,
	 PRIM_CLNT_SLCT_C,
	 GDW_EFFT_D
	)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	int_grup_i,
	paty_i,
	efft_d,
	expy_d,
	srce_syst_c,
	prim_clnt_f,
	rel_i,
	srce_syst_paty_int_grup_i,
	orig_srce_syst_paty_type_c,
	orig_srce_syst_paty_i,
	rel_c,
	vald_from_d,
	vald_to_d,
	prim_clnt_slct_c,
(
	SELECT
             "gdw_efft_d" FROM
             NPD_D12_DMN_GDWMIG_IBRG_V.PVDATA.GDW_EFFT_DATE
	) AS "gdw_efft_d"
FROM
	PVTECH.PATY_INT_GRUP
WHERE
	"gdw_efft_d" BETWEEN efft_d AND expy_d;

CREATE OR REPLACE VIEW PVDSTG.ACCT_PATY_DEDUP
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_DEDUP;

CREATE OR REPLACE VIEW PVDSTG.ACCT_PATY_REL_THA
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_REL_THA;

CREATE OR REPLACE VIEW PVDSTG.ACCT_PATY_REL_WSS
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_REL_WSS;

CREATE OR REPLACE VIEW PVDSTG.ACCT_PATY_THA_NEW_RNGE
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_THA_NEW_RNGE;

CREATE OR REPLACE VIEW PVDSTG.ACCT_REL_WSS_DITPS
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_REL_WSS_DITPS;

CREATE OR REPLACE VIEW PVDSTG.DERV_ACCT_PATY_ADD
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_ADD;

CREATE OR REPLACE VIEW PVDSTG.DERV_ACCT_PATY_CHG
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CHG;

CREATE OR REPLACE VIEW PVDSTG.DERV_ACCT_PATY_CURR
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CURR;

CREATE OR REPLACE VIEW PVDSTG.DERV_ACCT_PATY_DEL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_DEL;

CREATE OR REPLACE VIEW PVDSTG.DERV_ACCT_PATY_FLAG
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG;

CREATE OR REPLACE VIEW PVDSTG.DERV_ACCT_PATY_NON_RM
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_NON_RM;

CREATE OR REPLACE VIEW PVDSTG.DERV_ACCT_PATY_RM
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_RM;

CREATE OR REPLACE VIEW PVDSTG.DERV_ACCT_PATY_ROW_SECU_FIX
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_ROW_SECU_FIX;

CREATE OR REPLACE VIEW PVDSTG.DERV_PRTF_ACCT_PATY_PSST
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_PATY_PSST;

CREATE OR REPLACE VIEW PVDSTG.DERV_PRTF_ACCT_PATY_STAG
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_PATY_STAG;

CREATE OR REPLACE VIEW PVDSTG.DERV_PRTF_ACCT_STAG
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_STAG;

CREATE OR REPLACE VIEW PVDSTG.DERV_PRTF_PATY_STAG
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_PATY_STAG;

CREATE OR REPLACE VIEW PVDSTG.GRD_GNRC_MAP_BUSN_SEGM_PRTY
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_BUSN_SEGM_PRTY;

CREATE OR REPLACE VIEW PVDSTG.GRD_GNRC_MAP_DERV_PATY_HOLD
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_DERV_PATY_HOLD;

CREATE OR REPLACE VIEW PVDSTG.GRD_GNRC_MAP_DERV_PATY_REL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_DERV_PATY_REL;

CREATE OR REPLACE VIEW PVDSTG.GRD_GNRC_MAP_DERV_UNID_PATY
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_DERV_UNID_PATY;

CREATE OR REPLACE VIEW PVDSTG.GRD_GNRC_MAP_PATY_HOLD_PRTY
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_PATY_HOLD_PRTY;

CREATE OR REPLACE VIEW PVPATY.UTIL_PROS_ISAC
	(
	        PROS_KEY_I,
	        CONV_M,
	        CONV_TYPE_M,
	        PROS_RQST_S,
	        PROS_LAST_RQST_S,
	        PROS_RQST_Q,
	        BTCH_RUN_D,
	        BTCH_KEY_I,
	        SRCE_SYST_M,
	        SRCE_M,
	        TRGT_M,
	        SUCC_F,
	        COMT_F,
	        COMT_S,
	        MLTI_LOAD_EFFT_D,
	        SYST_S,
	        MLTI_LOAD_COMT_S,
	        SYST_ET_Q,
	        SYST_UV_Q,
	        SYST_INS_Q,
	        SYST_UPD_Q,
	        SYST_DEL_Q,
	        SYST_ET_TABL_M,
	        SYST_UV_TABL_M,
	        SYST_HEAD_ET_TABL_M,
	        SYST_HEAD_UV_TABL_M,
	        SYST_TRLR_ET_TABL_M,
	        SYST_TRLR_UV_TABL_M,
	        PREV_PROS_KEY_I,
	        HEAD_RECD_TYPE_C,
	        HEAD_FILE_M,
	        HEAD_BTCH_RUN_D,
	        HEAD_FILE_CRAT_S,
	        HEAD_GENR_PRGM_M,
	        HEAD_BTCH_KEY_I,
	        HEAD_PROS_KEY_I,
	        HEAD_PROS_PREV_KEY_I,
	        TRLR_RECD_TYPE_C,
	        TRLR_RECD_Q,
	        TRLR_HASH_TOTL_A,
	        TRLR_COLM_HASH_TOTL_M,
	        TRLR_EROR_RECD_Q,
	        TRLR_FILE_COMT_S,
	        TRLR_RECD_ISRT_Q,
	        TRLR_RECD_UPDT_Q,
	        TRLR_RECD_DELT_Q
	)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	
	PROS_KEY_I,
	CONV_M,
	CONV_TYPE_M,
	PROS_RQST_S,
	PROS_LAST_RQST_S,
	PROS_RQST_Q,
	BTCH_RUN_D,
	BTCH_KEY_I,
	SRCE_SYST_M,
	SRCE_M,
	TRGT_M,
	SUCC_F,
	COMT_F,
	COMT_S,
	MLTI_LOAD_EFFT_D,
	SYST_S,
	MLTI_LOAD_COMT_S,
	SYST_ET_Q,
	SYST_UV_Q,
	SYST_INS_Q,
	SYST_UPD_Q,
	SYST_DEL_Q,
	SYST_ET_TABL_M,
	SYST_UV_TABL_M,
	SYST_HEAD_ET_TABL_M,
	SYST_HEAD_UV_TABL_M,
	SYST_TRLR_ET_TABL_M,
	SYST_TRLR_UV_TABL_M,
	PREV_PROS_KEY_I,
	HEAD_RECD_TYPE_C,
	HEAD_FILE_M,
	HEAD_BTCH_RUN_D,
	HEAD_FILE_CRAT_S,
	HEAD_GENR_PRGM_M,
	HEAD_BTCH_KEY_I,
	HEAD_PROS_KEY_I,
	HEAD_PROS_PREV_KEY_I,
	TRLR_RECD_TYPE_C,
	TRLR_RECD_Q,
	TRLR_HASH_TOTL_A,
	TRLR_COLM_HASH_TOTL_M,
	TRLR_EROR_RECD_Q,
	TRLR_FILE_COMT_S,
	TRLR_RECD_ISRT_Q,
	TRLR_RECD_UPDT_Q,
	TRLR_RECD_DELT_Q
	
FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDPATY.UTIL_PROS_ISAC
	;

CREATE OR REPLACE VIEW PVTECH.ACCT_APPT_PDCT
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.ACCT_APPT_PDCT;

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
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
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
	
       FROM
             NPD_D12_DMN_GDWMIG_IBRG.PDPATY.ACCT_PATY
	) ACCT_PATY
WHERE
	(
/* Start - RLS */
	COALESCE("row_secu_accs_c",0) = 0 OR GETBIT( (
	SELECT
             try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
FROM
             NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
WHERE
             UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
),"row_secu_accs_c"
) = 1
/* End - RLS */
);

CREATE OR REPLACE VIEW PVTECH.ACCT_REL
             (
                     SUBJ_ACCT_I,
                     OBJC_ACCT_I,
                     REL_C,
                     EFFT_D,
                     EXPY_D,
                     STRT_D,
                     REL_EXPY_D,
                     PROS_KEY_EFFT_I,
                     PROS_KEY_EXPY_I,
                     EROR_SEQN_I,
                     ROW_SECU_ACCS_C,
                     REL_STUS_C,
                     SRCE_SYST_C,
                     SUBJ_ACCT_LEVL_N,
                     OBJC_ACCT_LEVL_N,
                     CRIN_SHRE_P,
                     DR_INT_SHRE_P,
                     RECORD_DELETED_FLAG,
                     CTL_ID,
                     PROCESS_NAME,
                     PROCESS_ID,
                     UPDATE_PROCESS_NAME,
                     UPDATE_PROCESS_ID
             )
             COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
             AS
             SELECT
	
	SUBJ_ACCT_I,
	OBJC_ACCT_I,
	REL_C,
	EFFT_D,
	EXPY_D,
	STRT_D,
	REL_EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	ROW_SECU_ACCS_C,
	REL_STUS_C,
	SRCE_SYST_C,
	SUBJ_ACCT_LEVL_N,
	OBJC_ACCT_LEVL_N,
	CRIN_SHRE_P,
	DR_INT_SHRE_P,
	RECORD_DELETED_FLAG,
	CTL_ID,
	PROCESS_NAME,
	PROCESS_ID,
	UPDATE_PROCESS_NAME,
	UPDATE_PROCESS_ID
	
       FROM
             NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.ACCT_REL
	) ACCT_REL
WHERE
(
/* Start - RLS */
	COALESCE("row_secu_accs_c",0) = 0 OR GETBIT( (
	SELECT
             -- row_secu_prfl_c
             try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
FROM
             NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
WHERE
             UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
),"row_secu_accs_c"
) = 1
/* End - RLS */
);

CREATE OR REPLACE VIEW PVTECH.ACCT_UNID_PATY
             (
                  ACCT_I,
             	 SRCE_SYST_PATY_I,
             	 SRCE_SYST_C,
             	 PATY_ACCT_REL_C,
             	 EFFT_D,
             	 EXPY_D,
             	 PROS_KEY_EFFT_I,
             	 PROS_KEY_EXPY_I,
             	 EROR_SEQN_I,
             	 ROW_SECU_ACCS_C,
             	 ORIG_SRCE_SYST_C
             )
             COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
             AS
             SELECT
	
	ACCT_I,
	SRCE_SYST_PATY_I,
	SRCE_SYST_C,
	PATY_ACCT_REL_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	ROW_SECU_ACCS_C,
	ORIG_SRCE_SYST_C
	
          FROM
             NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.ACCT_UNID_PATY
	) ACCT_UNID_PATY

Where (
/* Start - RLS */
	COALESCE("row_secu_accs_c",0) = 0 OR GETBIT( (
	SELECT
             -- row_secu_prfl_c
             try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
FROM
             NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
WHERE
             UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
),"row_secu_accs_c"
) = 1
/* End - RLS */
);

CREATE OR REPLACE VIEW PVTECH.ACCT_XREF_BPS_CBS
             COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
             AS
             -- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
             SELECT
	*
	 FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.ACCT_XREF_BPS_CBS;

CREATE OR REPLACE VIEW PVTECH.ACCT_XREF_MAS_DAR
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*
	 FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.ACCT_XREF_MAS_DAR;

CREATE OR REPLACE VIEW PVTECH.APPT
	(
	        APPT_I,
	        APPT_C,
	        APPT_FORM_C,
	        APPT_QLFY_C,
	        STUS_TRAK_I,
	        APPT_ORIG_C,
	        APPT_N,
	        SRCE_SYST_C,
	        SRCE_SYST_APPT_I,
	        APPT_CRAT_D,
	        RATE_SEEK_F,
	        PROS_KEY_EFFT_I,
	        EROR_SEQN_I,
	        ORIG_APPT_SRCE_C,
	        REL_MGR_STAT_C,
	        APPT_RECV_S,
	        APPT_RECV_D,
	        APPT_RECV_T,
	        APPT_ENTR_POIT_M,
	        EXT_TO_PDCT_SYST_D,
	        EFFT_D
	)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	
	APPT_I,
	APPT_C,
	APPT_FORM_C,
	APPT_QLFY_C,
	STUS_TRAK_I,
	APPT_ORIG_C,
	APPT_N,
	SRCE_SYST_C,
	SRCE_SYST_APPT_I,
	APPT_CRAT_D,
	RATE_SEEK_F,
	PROS_KEY_EFFT_I,
	EROR_SEQN_I,
	ORIG_APPT_SRCE_C,
	REL_MGR_STAT_C,
	APPT_RECV_S,
	APPT_RECV_D,
	APPT_RECV_T,
	APPT_ENTR_POIT_M,
	EXT_TO_PDCT_SYST_D,
	EFFT_D
	
FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.APPT
	;

CREATE OR REPLACE VIEW PVTECH.APPT_DEPT
	(
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
	)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	
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
	
FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.APPT_DEPT
	;

CREATE OR REPLACE VIEW PVTECH.APPT_PDCT
	(
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
	)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	
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
	
FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.APPT_PDCT
	;

CREATE OR REPLACE VIEW PVTECH.APPT_PDCT_FEAT
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.APPT_PDCT_FEAT;

CREATE OR REPLACE VIEW PVTECH.APPT_PDCT_REL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.APPT_PDCT_REL;

CREATE OR REPLACE VIEW PVTECH.APPT_REL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.APPT_REL;

CREATE OR REPLACE VIEW PVTECH.APPT_TRNF_DETL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.APPT_TRNF_DETL;

CREATE OR REPLACE VIEW PVTECH.BUSN_EVNT
	(
	        EVNT_I,
	        SRCE_SYST_EVNT_I,
	        EVNT_ACTL_D,
	        SRCE_SYST_C,
	        PROS_KEY_EFFT_I,
	        EROR_SEQN_I,
	        SRCE_SYST_EVNT_TYPE_I,
	        EVNT_ACTL_T,
	        ROW_SECU_ACCS_C,
	        EVNT_ACTV_TYPE_C,
	        EFFT_D,
	        EXPY_D,
	        RECORD_DELETED_FLAG,
	        CTL_ID,
	        PROCESS_NAME,
	        PROCESS_ID,
	        UPDATE_PROCESS_NAME,
	        UPDATE_PROCESS_ID
	)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	
	EVNT_I,
	SRCE_SYST_EVNT_I,
	EVNT_ACTL_D,
	SRCE_SYST_C,
	PROS_KEY_EFFT_I,
	EROR_SEQN_I,
	SRCE_SYST_EVNT_TYPE_I,
	EVNT_ACTL_T,
	ROW_SECU_ACCS_C,
	EVNT_ACTV_TYPE_C,
	EFFT_D,
	EXPY_D,
	RECORD_DELETED_FLAG,
	CTL_ID,
	PROCESS_NAME,
	PROCESS_ID,
	UPDATE_PROCESS_NAME,
	UPDATE_PROCESS_ID
	
FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.BUSN_EVNT
WHERE

((
/* Start - RLS */
	COALESCE("row_secu_accs_c",0) = 0 OR GETBIT( (
	SELECT
             -- "row_secu_prfl_c"
             try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
FROM
             NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
WHERE
             UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
),"row_secu_accs_c"
) = 1
/* End - RLS */
));



CREATE OR REPLACE VIEW Sys_Calendar.CALBASICS
	(
	  calendar_date,
	  day_of_calendar,
	  day_of_month,
	  day_of_year,
	  month_of_year,
	  year_of_calendar)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	  "cdate",
	  CASE
	    WHEN (((MOD(TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD')), 10000)) / 100) > 2)
	    THEN
	      (146097 * ((TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD'))/10000 + 1900) / 100)) / 4
	      +(1461 * ((TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD'))/10000 + 1900) - ((TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD'))/10000 + 1900) / 100)*100) ) / 4
	      +(153 * (((MOD(TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD')), 10000))/100) - 3) + 2) / 5
	      + MOD( TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD')), 100) - 693901
	  ELSE
	    (146097 * (((TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD'))/10000 + 1900) - 1) / 100)) / 4
	    +(1461 * (((TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD'))/10000 + 1900) - 1) - (((TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD'))/10000 + 1900) - 1) / 100)*100) ) / 4
	    +(153 * (((MOD(TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD')), 10000))/100) + 9) + 2) / 5
	    + MOD( TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD')), 100) - 693901
	  END,
	  MOD(TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD')), 100),
	  (CASE
	    (MOD(TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD')), 10000))/100
	    WHEN 1
	    THEN MOD(TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD')), 100)
	    WHEN 2
	    THEN MOD(TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD')), 100) + 31
	    WHEN 3
	    THEN MOD(TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD')), 100) + 59
	    WHEN 4
	    THEN MOD(TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD')), 100) + 90
	    WHEN 5
	    THEN MOD(TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD')), 100) + 120
	    WHEN 6
	    THEN MOD(TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD')), 100) + 151
	    WHEN 7
	    THEN MOD(TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD')), 100) + 181
	    WHEN 8
	    THEN MOD(TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD')), 100) + 212
	    WHEN 9
	    THEN MOD(TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD')), 100) + 243
	    WHEN 10
	    THEN MOD(TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD')), 100) + 273
	    WHEN 11
	    THEN MOD(TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD')), 100) + 304
	    WHEN 12
	    THEN MOD(TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD')), 100) + 334
	  END)
	  +
	  (CASE
	    WHEN
	    (
	      (
	        (MOD((TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD')) / 10000 + 1900), 4) = 0)
	        AND
	        (MOD((TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD')) / 10000 + 1900), 100) <> 0)
	      )
	      OR
	      (MOD((TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD')) / 10000 + 1900), 400) = 0)
	    )
	    AND
	    (
	      (MOD(TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD')), 10000))/100 > 2
	    )
	    THEN 1
	    ELSE 0
	  END),
	  (MOD(TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD')), 10000))/100,
	  TO_NUMBER(TO_CHAR("cdate", 'YYYYMMDD'))/10000
	FROM
	  NPD_D12_DMN_GDWMIG_IBRG.Sys_Calendar.CALDATES;


CREATE OR REPLACE VIEW PVTECH.CALENDAR
             COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
             AS
             -- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
             SELECT	*
             FROM  NPD_D12_DMN_GDWMIG_IBRG_V.Sys_Calendar.CALBASICS;
			 --NPD_D12_DMN_GDWMIG_IBRG.Sys_Calendar.CALENDAR;

CREATE OR REPLACE VIEW PVTECH.CLS_FCLY
             COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
             AS
             -- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
             SELECT
	*
	 FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.CLS_FCLY;

CREATE OR REPLACE VIEW PVTECH.CLS_UNID_PATY
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*
	 FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.CLS_UNID_PATY;

CREATE OR REPLACE VIEW PVTECH.DAR_ACCT
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*
	 FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DAR_ACCT;

CREATE OR REPLACE VIEW PVTECH.DEPT_DIMN_NODE_ANCS_CURR
	(
	 DEPT_I
	,ANCS_DEPT_I
	,ANCS_LEVL_N
	,AS_AT_D
	)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	
	DEPT_I,
	ANCS_DEPT_I,
	ANCS_LEVL_N,
	AS_AT_D
	
FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDGRD.DEPT_DIMN_NODE_ANCS_CURR T1;

CREATE OR REPLACE VIEW PVTECH.DERV_ACCT_PATY
	(
	 ACCT_I
	,PATY_I
	,ASSC_ACCT_I
	,PATY_ACCT_REL_C
	,PRFR_PATY_F
	,SRCE_SYST_C
	,EFFT_D
	,EXPY_D
	,PROS_KEY_EFFT_I
	,PROS_KEY_EXPY_I
	,ROW_SECU_ACCS_C
	)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	
	ACCT_I,
	PATY_I,
	ASSC_ACCT_I,
	PATY_ACCT_REL_C,
	PRFR_PATY_F,
	SRCE_SYST_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	ROW_SECU_ACCS_C
	

            FROM
             NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_ACCT_PATY
	) DERV_ACCT_PATY
WHERE
	       (
/* Start - RLS */
	COALESCE("row_secu_accs_c",0) = 0 OR GETBIT( (
	SELECT
             -- "row_secu_prfl_c"
             try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
FROM
             NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
WHERE
             UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
),"row_secu_accs_c"
) = 1
/* End - RLS */
);


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
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
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
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_ACCT_REL

WHERE (
(
/* Start - RLS */
	COALESCE("row_secu_accs_c",0) = 0 OR GETBIT( (
	SELECT
             -- "row_secu_prfl_c"
             try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
FROM
             NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
WHERE
             UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
),"row_secu_accs_c"
) = 1
/* End - RLS */
)
);
CREATE OR REPLACE VIEW PVTECH.DERV_PRTF_ACCT
             -- --- ---------- ---------------------------------------------------------
             -- Ver Date       Modified By         Description
             -- --- ---------- ---------------------------------------------------------
             --
             -- 1.0  26/06/2013 T Jelliffe          Initial Version
             -- 1.1  28/06/2013 T Jelliffe          Use duration persist table
             -- 1.2  12/07/2013 T Jelliffe          Time period reduced 15 to 3 years
             -- 1.3  17/07/2013 T Jelliffe          39 months history range
             -- 1.4  25/07/2013 T Jelliffe          Date join on JOIN_FROM_D and TO_D
             -- 1.5  14/01/2014 H Zak               read from the corresponding 1:1 views over the new relationship tables
             ---------------------------------------------------------------------------
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
             COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
             AS
             SELECT
	CALR.calendar_date AS "perd_d",
	vald_from_d,
	vald_to_d,
	efft_d,
	expy_d,
	acct_i,
	int_grup_i,
	derv_prtf_catg_c,
	derv_prtf_clas_c,
	derv_prtf_type_c,
	ptcl_n,
	rel_mnge_i,
	prtf_code_x,
	srce_syst_c,
	row_secu_accs_c
FROM
	PVTECH.DERV_PRTF_ACCT_REL T1
	INNER JOIN
	PVTECH.CALENDAR CALR
	ON CALR.calendar_date BETWEEN T1.vald_from_d AND T1.vald_to_d
	AND CALR.calendar_date BETWEEN DATEADD(MONTH, -39, (CURRENT_DATE() - EXTRACT(DAY FROM CURRENT_DATE()) +1 ))
	AND DATEADD(MONTH, 1, CURRENT_DATE())
;


CREATE OR REPLACE VIEW PVTECH.DERV_PRTF_OWN_REL
             (
                     INT_GRUP_I,
             	 DERV_PRTF_TYPE_C,
             	 DERV_PRTF_CATG_C,
             	 DERV_PRTF_CLAS_C,
             	 VALD_FROM_D,
             	 VALD_TO_D,
             	 EFFT_D,
             	 EXPY_D,
             	 PTCL_N,
             	 REL_MNGE_I,
             	 PRTF_CODE_X,
             	 DERV_PRTF_ROLE_C,
             	 ROLE_PLAY_TYPE_X,
             	 ROLE_PLAY_I,
             	 SRCE_SYST_C,
             	 ROW_SECU_ACCS_C
             )
             COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
             AS
             SELECT
	
	INT_GRUP_I,
	DERV_PRTF_TYPE_C,
	DERV_PRTF_CATG_C,
	DERV_PRTF_CLAS_C,
	VALD_FROM_D,
	VALD_TO_D,
	EFFT_D,
	EXPY_D,
	PTCL_N,
	REL_MNGE_I,
	PRTF_CODE_X,
	DERV_PRTF_ROLE_C,
	ROLE_PLAY_TYPE_X,
	ROLE_PLAY_I,
	SRCE_SYST_C,
	ROW_SECU_ACCS_C
	
FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_OWN_REL

WHERE (
(
/* Start - RLS */
	COALESCE("row_secu_accs_c",0) = 0 OR GETBIT( (
	SELECT
             -- "row_secu_prfl_c"
             try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
FROM
             PVSECURITY.ROW_LEVL_SECU_USER_PRFL
WHERE
             UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
),"row_secu_accs_c"
) = 1
/* End - RLS */
)
);



CREATE OR REPLACE VIEW PVTECH.DERV_PRTF_PATY_REL
	(
	        PATY_I,
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
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	
	PATY_I,
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
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_PATY_REL

WHERE (
(
/* Start - RLS */
	COALESCE("row_secu_accs_c",0) = 0 OR GETBIT( (
	SELECT
             -- "row_secu_prfl_c"
             try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
FROM
             NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
WHERE
             UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
),"row_secu_accs_c"
) = 1
/* End - RLS */
)
);
CREATE OR REPLACE VIEW PVTECH.DERV_PRTF_PATY
             -- --- ---------- ---------------------------------------------------------
             -- Ver Date       Modified By         Description
             -- --- ---------- ---------------------------------------------------------
             --
             -- 1.0  27/06/2013 T Jelliffe          Initial Version
             -- 1.1  28/06/2013 T Jelliffe          Use duration persist table
             -- 1.2  12/07/2013 T Jelliffe          Time period reduced 15 to 3 years
             -- 1.3  17/07/2013 T Jelliffe          39 months history range
             -- 1.4  25/07/2013 T Jelliffe          Date join on JOIN_FROM_D and TO_D
             -- 1.5  14/01/2014 H Zak               read from the corresponding 1:1 views over the new relationship tables
             ---------------------------------------------------------------------------
             (
              PERD_D
             ,VALD_FROM_D
             ,VALD_TO_D
             ,EFFT_D
             ,EXPY_D
             ,PATY_I
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
             COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
             AS
             SELECT
	CALR.calendar_date AS   perd_d,
	vald_from_d,
	vald_to_d,
	efft_d,
	expy_d,
	paty_i,
	int_grup_i,
	derv_prtf_catg_c,
	derv_prtf_clas_c,
	derv_prtf_type_c,
	ptcl_n,
	rel_mnge_i,
	prtf_code_x,
	srce_syst_c,
	row_secu_accs_c
FROM
	PVTECH.DERV_PRTF_PATY_REL T1
	 INNER JOIN
	PVTECH.CALENDAR CALR
	 ON CALR.calendar_date BETWEEN T1.vald_from_d AND T1.vald_to_d
	 AND CALR.calendar_date BETWEEN DATEADD(MONTH, -39, (CURRENT_DATE() - EXTRACT(DAY FROM CURRENT_DATE()) +1 ))
	 AND DATEADD(MONTH, 1, CURRENT_DATE())
	;


CREATE OR REPLACE VIEW PVTECH.EVNT
             (
                     EVNT_I,
                     EVNT_ACTV_TYPE_C,
                     INVT_EVNT_F,
                     FNCL_ACCT_EVNT_F,
                     CTCT_EVNT_F,
                     BUSN_EVNT_F,
                     PROS_KEY_EFFT_I,
                     EROR_SEQN_I,
                     FNCL_NVAL_EVNT_F,
                     INCD_F,
                     INSR_EVNT_F,
                     INSR_NVAL_EVNT_F,
                     ROW_SECU_ACCS_C,
                     FNCL_GL_EVNT_F,
                     AUTT_AUTN_EVNT_F,
                     COLL_EVNT_F,
                     SRCE_SYST_C,
                     EVNT_REAS_C,
                     EFFT_D,
                     EXPY_D,
                     RECORD_DELETED_FLAG,
                     CTL_ID,
                     PROCESS_NAME,
                     PROCESS_ID,
                     UPDATE_PROCESS_NAME,
                     UPDATE_PROCESS_ID
             )
             COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
             AS
             SELECT
	
	EVNT_I,
	EVNT_ACTV_TYPE_C,
	INVT_EVNT_F,
	FNCL_ACCT_EVNT_F,
	CTCT_EVNT_F,
	BUSN_EVNT_F,
	PROS_KEY_EFFT_I,
	EROR_SEQN_I,
	FNCL_NVAL_EVNT_F,
	INCD_F,
	INSR_EVNT_F,
	INSR_NVAL_EVNT_F,
	ROW_SECU_ACCS_C,
	FNCL_GL_EVNT_F,
	AUTT_AUTN_EVNT_F,
	COLL_EVNT_F,
	SRCE_SYST_C,
	EVNT_REAS_C,
	EFFT_D,
	EXPY_D,
	RECORD_DELETED_FLAG,
	CTL_ID,
	PROCESS_NAME,
	PROCESS_ID,
	UPDATE_PROCESS_NAME,
	UPDATE_PROCESS_ID
	
FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.EVNT
WHERE
((
/* Start - RLS */
	COALESCE("row_secu_accs_c",0) = 0 OR GETBIT( (
	SELECT
             -- "row_secu_prfl_c"
             try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
FROM
             NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL 
WHERE
             UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
),"row_secu_accs_c"
) = 1
/* End - RLS */
));

CREATE OR REPLACE VIEW PVTECH.EVNT_EMPL
             COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
             AS
             SELECT
	
	EVNT_I,
	EMPL_I,
	EVNT_PATY_ROLE_TYPE_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	ROW_SECU_ACCS_C
	
            FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.EVNT_EMPL
            WHERE
            ("row_secu_accs_c" = 0)
            /*
            -- SNOWFLAKE DOES NOT SUPPORT PROFILES, REFERENCING ROLE INSTEAD 
            The following code will need to be updated to use the new role based security model.
            OR
            ((MOD(SUBSTR(CURRENT_ROLE(), 2, 3), 2) = 1) AND (MOD(ROW_SECU_ACCS_C, 2) = 1))  -- SNOWFLAKE DOES NOT SUPPORT PROFILES, REFERENCING ROLE INSTEAD 
            OR
            ((MOD((SUBSTR(CURRENT_ROLE(), 2, 3) /2), 2) = 1) AND (MOD((TRUNC(ROW_SECU_ACCS_C/2)), 2) = 1))  -- SNOWFLAKE DOES NOT SUPPORT PROFILES, REFERENCING ROLE INSTEAD
            */
     ;

CREATE OR REPLACE VIEW PVTECH.EVNT_INT_GRUP
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.EVNT_INT_GRUP;

CREATE OR REPLACE VIEW PVTECH.GRD_DEPT_FLAT_CURR
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*
from
	NPD_D12_DMN_GDWMIG_IBRG.PDGRD.GRD_DEPT_FLAT_CURR;

CREATE OR REPLACE VIEW PVTECH.GRD_GNRC_MAP
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDGRD.GRD_GNRC_MAP;

CREATE OR REPLACE VIEW PVTECH.GRD_GNRC_MAP_CURR
	(
	  MAP_TYPE_C
	, EFFT_D
	, TARG_NUMC_C
	, TARG_CHAR_C
	, SRCE_NUMC_1_C
	, SRCE_CHAR_1_C
	, SRCE_NUMC_2_C
	, SRCE_CHAR_2_C
	, SRCE_NUMC_3_C
	, SRCE_CHAR_3_C
	, SRCE_NUMC_4_C
	, SRCE_CHAR_4_C
	, SRCE_NUMC_5_C
	, SRCE_CHAR_5_C
	, SRCE_NUMC_6_C
	, SRCE_CHAR_6_C
	, SRCE_NUMC_7_C
	, SRCE_CHAR_7_C
	, EXPY_D
	, GDW_EFFT_D)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	"map_type_c",
	"efft_d",
	"targ_numc_c",
	"targ_char_c",
	"srce_numc_1_c",
	"srce_char_1_c",
	"srce_numc_2_c",
	"srce_char_2_c",
	"srce_numc_3_c",
	"srce_char_3_c",
	"srce_numc_4_c",
	"srce_char_4_c",
	"srce_numc_5_c",
	"srce_char_5_c",
	"srce_numc_6_c",
	"srce_char_6_c",
	"srce_numc_7_c",
	"srce_char_7_c",
	"expy_d",
	DT1."gdw_efft_d"
FROM
	PVTECH.GRD_GNRC_MAP,
	(
	SELECT
             "gdw_efft_d" AS "gdw_efft_d"
	  FROM
             PVDATA.GDW_EFFT_DATE
	) AS DT1
WHERE DT1."gdw_efft_d"
BETWEEN "efft_d" AND "expy_d";

CREATE OR REPLACE VIEW PVTECH.GRD_GNRC_MAP_DERV_PATY_HOLD
	(
	MAP_TYPE_C
	,PATY_ACCT_REL_X
	,PATY_ACCT_REL_C
	,EFFT_D
	,EXPY_D
	)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	map_type_c,
	targ_char_c AS "paty_acct_rel_x",
	srce_char_1_c AS "paty_acct_rel_c",
	efft_d,
	expy_d
	FROM
	PVTECH.GRD_GNRC_MAP_CURR
	WHERE
	UPPER(RTRIM( map_type_c)) = UPPER(RTRIM('DERV_ACCT_PATY_HOLD_REL_MAP'));
CREATE OR REPLACE VIEW PVTECH.GRD_GNRC_MAP_DERV_PATY_REL
	(
	MAP_TYPE_C
	,SRCE_SYST_C
	,REL_C
	,ACCT_I_C
	,EFFT_D
	,EXPY_D
	)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	map_type_c,
	targ_char_c AS "srce_syst_c",
	srce_char_1_c AS "rel_c",
	srce_char_2_c AS "acct_i_c",
	efft_d,
	expy_d
	FROM
	PVTECH.GRD_GNRC_MAP_CURR
	WHERE
	UPPER(RTRIM( map_type_c)) = UPPER(RTRIM('DERV_ACCT_PATY_ACCT_REL_MAP'));

--Create new technical view PVTECH.GRD_GNRC_MAP_DERV_UNID_PATY

CREATE OR REPLACE VIEW PVTECH.GRD_GNRC_MAP_DERV_UNID_PATY
	(
	MAP_TYPE_C
	,SRCE_SYST_C
	,UNID_PATY_SRCE_SYST_C
	,UNID_PATY_ACCT_REL_C
	,EFFT_D
	,EXPY_D
	)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	map_type_c,
	targ_char_c AS "srce_syst_c",
	srce_char_1_c AS "unid_paty_srce_syst_c",
	srce_char_2_c AS "unid_paty_acct_rel_c",
	efft_d,
	expy_d
	FROM
	PVTECH.GRD_GNRC_MAP_CURR
	WHERE
	UPPER(RTRIM( map_type_c)) = UPPER(RTRIM('DERV_ACCT_PATY_UNID_PATY_MAP'));
--Replace PVTECH.INT_GRUP:

CREATE OR REPLACE VIEW PVTECH.INT_GRUP
	(
	        INT_GRUP_I,
		 INT_GRUP_TYPE_C,
		 SRCE_SYST_INT_GRUP_I,
		 SRCE_SYST_C,
		 EFFT_D,
		 EXPY_D,
		 PROS_KEY_EFFT_I,
		 PROS_KEY_EXPY_I,
		 EROR_SEQN_I,
		 INT_GRUP_M,
		 ORIG_SRCE_SYST_INT_GRUP_I,
		 CRAT_D,
		 QLFY_C,
		 PTCL_N,
		 REL_MNGE_I,
		 VALD_TO_D,
		 ROW_SECU_ACCS_C,
		 INT_GRUP_CATG_C,
		 ISO_CNTY_C
	)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	
	INT_GRUP_I,
	INT_GRUP_TYPE_C,
	SRCE_SYST_INT_GRUP_I,
	SRCE_SYST_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	INT_GRUP_M,
	ORIG_SRCE_SYST_INT_GRUP_I,
	CRAT_D,
	QLFY_C,
	PTCL_N,
	REL_MNGE_I,
	VALD_TO_D,
	ROW_SECU_ACCS_C,
	INT_GRUP_CATG_C,
	ISO_CNTY_C
	
FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.INT_GRUP

Where (
/* Start - RLS */
	COALESCE("row_secu_accs_c",0) = 0 OR GETBIT( (
	SELECT
            try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
FROM
             NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
WHERE
             UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
),"row_secu_accs_c"
) = 1
/* End - RLS */
);


--Replace existing View

CREATE OR REPLACE VIEW PVTECH.INT_GRUP_EMPL
             (
                     INT_GRUP_I,
                     EMPL_I,
                     EMPL_ROLE_C,
                     EFFT_D,
                     EXPY_D,
                     PROS_KEY_EFFT_I,
                     PROS_KEY_EXPY_I,
                     EROR_SEQN_I,
                     SRCE_SYST_C,
                     VALD_FROM_D,
                     VALD_TO_D,
                     ROW_SECU_ACCS_C
             )
             COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
             AS
             SELECT
	
	INT_GRUP_I,
	EMPL_I,
	EMPL_ROLE_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	REL_C,
	VALD_FROM_D,
	VALD_TO_D,
	ROW_SECU_ACCS_C,
	SRCE_SYST_C
	
FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.INT_GRUP_EMPL
WHERE
(
/* Start - RLS */
	COALESCE("row_secu_accs_c",0) = 0 OR GETBIT( (
	SELECT
            try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
FROM
             NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
WHERE
             UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
),"row_secu_accs_c"
) = 1
/* End - RLS */
);

CREATE OR REPLACE VIEW PVTECH.INT_GRUP_UNID_PATY
             COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
             AS
             -- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
             SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.INT_GRUP_UNID_PATY;

CREATE OR REPLACE VIEW PVTECH.MAP_CMS_PDCT
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*
	FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDCMS.MAP_CMS_PDCT;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_ADRS_TYPE
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*  FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_ADRS_TYPE
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_ACQR_SRCE
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_ACQR_SRCE;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_C
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_C;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_CMPE
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_CMPE;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_CODE_HM
	(
	        HLM_APPT_TYPE_CATG_I,
	        APPT_C,
	        EFFT_D,
	        EXPY_D
	)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	
	HLM_APPT_TYPE_CATG_I,
	APPT_C,
	EFFT_D,
	EXPY_D
	
FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_CODE_HM
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_COND
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*  FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_COND
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_DOCU_DELY
	(
	        EXEC_DOCU_RECV_TYPE,
	        DOCU_DELY_RECV_C,
	        EFFT_D,
	        EXPY_D
	)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	
	EXEC_DOCU_RECV_TYPE,
	DOCU_DELY_RECV_C,
	EFFT_D,
	EXPY_D
	
FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_DOCU_DELY
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_FEAT
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_FEAT;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_FORM
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_FORM;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_ORIG
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_ORIG;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_PDCT_FEAT
	(
	        PEXA_FLAG,
	 FEAT_VALU_C,
	 EFFT_D,
	 EXPY_D
	)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	
	PEXA_FLAG,
	FEAT_VALU_C,
	EFFT_D,
	EXPY_D
	
FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_PDCT_FEAT

	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_PDCT_PATY_ROLE
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_PDCT_PATY_ROLE;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_PURP_CL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_PURP_CL;


CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_PURP_CLAS_CL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_PURP_CLAS_CL;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_PURP_HL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_PURP_HL;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_PURP_PL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_PURP_PL;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_QLFY
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*  FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_QLFY
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_QSTN_HL
	(
	        QA_QUESTION_ID,
	        QSTN_C,
	        EFFT_D,
	        EXPY_D
	)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	
	QA_QUESTION_ID,
	QSTN_C,
	EFFT_D,
	EXPY_D
	
FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_QSTN_HL
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_QSTN_RESP_HL
	(
	        QA_ANSWER_ID,
	        RESP_C,
	        EFFT_D,
	        EXPY_D
	)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	
	QA_ANSWER_ID,
	RESP_C,
	EFFT_D,
	EXPY_D
	
FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_QSTN_RESP_HL
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_CMPE_IDNN
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*  FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_CMPE_IDNN
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_CNTY
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*  FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_CNTY
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_CRIS_PDCT
	(
	        CRIS_PDCT_ID,
	        ACCT_QLFY_C,
	        SRCE_SYST_C,
	        EFFT_D,
	        EXPY_D
	)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	
	CRIS_PDCT_ID,
	ACCT_QLFY_C,
	SRCE_SYST_C,
	EFFT_D,
	EXPY_D
	
FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_CRIS_PDCT
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_DOCU_METH
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*  FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_DOCU_METH
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_ENV_CHLD_PATY_REL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_ENV_CHLD_PATY_REL;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_ENV_EVNT_ACTV_TYPE
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_ENV_EVNT_ACTV_TYPE;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_ENV_PATY_REL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_ENV_PATY_REL;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_ENV_PATY_TYPE
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_ENV_PATY_TYPE;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_FEAT_OVRD_REAS_HL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_FEAT_OVRD_REAS_HL;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_FEAT_OVRD_REAS_HL_D
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_FEAT_OVRD_REAS_HL_D;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_FEAT_OVRD_REAS_PL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_FEAT_OVRD_REAS_PL;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_FEE_CAPL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_FEE_CAPL;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_FNDD_METH
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*  FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_FNDD_METH
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_JOB_COMM_CATG
	(
	     CLP_JOB_FAMILY_CAT_ID,
	     JOB_COMM_CATG_C,
	     EFFT_D,
	     EXPY_D)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	
	CLP_JOB_FAMILY_CAT_ID,
	JOB_COMM_CATG_C,
	EFFT_D,
	EXPY_D
	
	 FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_JOB_COMM_CATG
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_LOAN_FNDD_METH
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_LOAN_FNDD_METH;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_LOAN_TERM_PL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_LOAN_TERM_PL;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_LPC_DEPT_HL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_LPC_DEPT_HL;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_ORIG_APPT_SRCE
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*
	 FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_ORIG_APPT_SRCE;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_ORIG_APPT_SRCE_HM
	(
	        HL_BUSN_CHNL_CAT_I,
	        ORIG_APPT_SRCE_SYST_C,
	        EFFT_D,
	        EXPY_D
	)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	
	HL_BUSN_CHNL_CAT_I,
	ORIG_APPT_SRCE_SYST_C,
	EFFT_D,
	EXPY_D
	
FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_ORIG_APPT_SRCE_HM
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_OVRD_FEE_FRQ_CL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_OVRD_FEE_FRQ_CL;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_PACK_PDCT_HL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_PACK_PDCT_HL;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_PACK_PDCT_PL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_PACK_PDCT_PL;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_PAYT_FREQ
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	-- NPD_D12_DMN_GDWMIG_IBRG.MAP_CSE_PAYT_FREQ.MAP_CSE_PAYT_FREQ;
    NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_PAYT_FREQ; --REPLACED SCHEMA NAME BY MH

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_PDCT_REL_CL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_PDCT_REL_CL;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_PL_ACQR_TYPE
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_PL_ACQR_TYPE;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_SM_CASE_STUS
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_SM_CASE_STUS;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_SM_CASE_STUS_REAS
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_SM_CASE_STUS_REAS;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_STATE
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*  FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_STATE
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_TRNF_OPTN
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_TRNF_OPTN;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_TU_APPT_C
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*  FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_TU_APPT_C
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_UNID_PATY_CATG_PL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_UNID_PATY_CATG_PL;

CREATE OR REPLACE VIEW PVTECH.MOS_FCLY
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*  FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MOS_FCLY;

--Note:: Table count should match for DBA 2.1 and DBA 2.6
	--Create the following Views for the above GDW tables

CREATE OR REPLACE VIEW PVTECH.MOS_LOAN
	(
	     LOAN_I,
	  FCLY_I,
	  MOS_APPT_C,
	  MOS_ASET_LIBL_C,
	  MOS_STUS_C,
	  ACRL_X,
	  TRAD_CNCY_ORIG_A,
	  TRAD_CNCY_SETL_A,
	  TRAD_CNCY_CURR_BALN_A,
	  INT_R,
	  FIX_INT_R,
	  VARY_INT_R,
	  ISSU_D,
	  MTUR_D,
	  ROLV_D,
	  INT_PAYT_FREQ_C,
	  NEXT_INT_D,
	  TRAD_CNCY_ACRE_A,
	  LAST_INT_PAYT_D,
	  TRAD_CNCY_TOTL_INT_RECV_A,
	  PDCT_N,
	  TRAD_CNCY_C,
	  BASE_CNCY_C,
	  TRAD_CNCY_INT_A,
	  AUD_CNCY_INT_A,
	  BASE_CNCY_INT_A,
	  TRAD_CNCY_AVRG_BALN_A,
	  AUD_CNCY_AVRG_BALN_A,
	  BASE_CNCY_AVRG_BALN_A,
	  TRAD_CNCY_MARG_A,
	  AUD_CNCY_MARG_A,
	  BASE_CNCY_MARG_A,
	  AVRG_MARG_R,
	  DEPT_I,
	  MSA_BALN_GL_ACCT_I,
	  MSA_INT_GL_ACCT_I,
	  EFFT_D,
	  EXPY_D,
	  PROS_KEY_EFFT_I,
	  PROS_KEY_EXPY_I,
	  PROF_CNTR_CODE_X
	 )
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	
	LOAN_I,
	FCLY_I,
	MOS_APPT_C,
	MOS_ASET_LIBL_C,
	MOS_STUS_C,
	ACRL_X,
	TRAD_CNCY_ORIG_A,
	TRAD_CNCY_SETL_A,
	TRAD_CNCY_CURR_BALN_A,
	INT_R,
	FIX_INT_R,
	VARY_INT_R,
	ISSU_D,
	MTUR_D,
	ROLV_D,
	INT_PAYT_FREQ_C,
	NEXT_INT_D,
	TRAD_CNCY_ACRE_A,
	LAST_INT_PAYT_D,
	TRAD_CNCY_TOTL_INT_RECV_A,
	PDCT_N,
	TRAD_CNCY_C,
	BASE_CNCY_C,
	TRAD_CNCY_INT_A,
	AUD_CNCY_INT_A,
	BASE_CNCY_INT_A,
	TRAD_CNCY_AVRG_BALN_A,
	AUD_CNCY_AVRG_BALN_A,
	BASE_CNCY_AVRG_BALN_A,
	TRAD_CNCY_MARG_A,
	AUD_CNCY_MARG_A,
	BASE_CNCY_MARG_A,
	AVRG_MARG_R,
	DEPT_I,
	MSA_BALN_GL_ACCT_I,
	MSA_INT_GL_ACCT_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	PROF_CNTR_CODE_X
	
FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MOS_LOAN;

CREATE OR REPLACE VIEW PVTECH.NON_WORK_DAY
	(
	        GEOA_TYPE_C,
	        GEOA_C,
	        NON_WORK_D,
	        NON_WORK_DAY_TYPE_C,
	        NON_WORK_DAY_M,
	        HLDY_STUS_X
	)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	
	GEOA_TYPE_C,
	GEOA_C,
	NON_WORK_D,
	NON_WORK_DAY_TYPE_C,
	NON_WORK_DAY_M,
	HLDY_STUS_X
	
FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDGRD.NON_WORK_DAY
	;


CREATE OR REPLACE VIEW PVTECH.ODS_RULE
	(
	   RULE_CODE,
	   RULE_STEP_SEQN,
	   PRTY,
	   VALD_FROM,
	   VALD_TO,
	   RULE_DESN,
	   RULE_STEP_DESN,
	   LKUP1_TEXT,
	   LKUP1_NUMB,
	   LKUP1_DATE,
	   LKUP1_ADD_ATTR,
	   LKUP2_TEXT,
	   LKUP2_NUMB,
	   LKUP2_DATE,
	   LKUP2_ADD_ATTR,
	   RULE_CMMT,
	   UPDT_DTTS,
	   CRAT_DTTS
	)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	(
	SELECT
	rule_code,
	rule_step_seqn,
	prty,
	vald_from,
	vald_to,
	rule_desn,
	rule_step_desn,
	lkup1_text,
	lkup1_numb,
	lkup1_date,
	lkup1_add_attr,
	lkup2_text,
	lkup2_numb,
	lkup2_date,
	lkup2_add_attr,
	rule_cmmt,
	updt_dtts,
	crat_dtts
FROM
	PVCBODS.ODS_RULE
	);

CREATE OR REPLACE VIEW PVTECH.PATY_APPT_PDCT
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.PATY_APPT_PDCT;

--Replace PVTECH.PATY_REL:

CREATE OR REPLACE VIEW PVTECH.PATY_REL
             (
                     PATY_I,
             	 RELD_PATY_I,
             	 REL_I,
             	 REL_REAS_C,
             	 REL_TYPE_C,
             	 SRCE_SYST_C,
             	 PATY_ROLE_C,
             	 REL_STUS_C,
             	 REL_LEVL_C,
             	 REL_EFFT_D,
             	 REL_EXPY_D,
             	 SRCE_SYST_REL_I,
             	 EFFT_D,
             	 EXPY_D,
             	 PROS_KEY_EFFT_I,
             	 PROS_KEY_EXPY_I,
             	 ROW_SECU_ACCS_C,
             	 RISK_AGGR_F
             )
             COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
             AS
             SELECT
	
	PATY_I,
	RELD_PATY_I,
	REL_I,
	REL_REAS_C,
	REL_TYPE_C,
	SRCE_SYST_C,
	PATY_ROLE_C,
	REL_STUS_C,
	REL_LEVL_C,
	REL_EFFT_D,
	REL_EXPY_D,
	SRCE_SYST_REL_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	ROW_SECU_ACCS_C,
	RISK_AGGR_F
	
FROM
	NPD_D12_DMN_GDWMIG_IBRG.PDPATY.PATY_REL

Where (
/* Start - RLS */
	COALESCE("row_secu_accs_c",0) = 0 OR GETBIT( (
	SELECT
            --  "row_secu_prfl_c"
			try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
FROM
             NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
WHERE
             UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
),"row_secu_accs_c"
) = 1
/* End - RLS */
);

CREATE OR REPLACE VIEW PVTECH.THA_ACCT
             (
              THA_ACCT_I,
              ACCT_QLFY_C,
              EXT_D,
              CSL_CLNT_I,
              TRAD_ACCT_I,
              THA_ACCT_TYPE_C,
              THA_ACCT_STUS_C,
              PALL_BUSN_UNIT_I,
              PALL_DEPT_I,
              BALN_A,
              IACR_MTD_A,
              IACR_FYTD_A,
              DALY_AGGR_FEE_A,
              BALN_D,
              EFFT_D,
              EXPY_D,
              PROS_KEY_EFFT_I,
              PROS_KEY_EXPY_I
             )
             COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
             AS
             SELECT
	
	THA_ACCT_I,
	ACCT_QLFY_C,
	EXT_D,
	CSL_CLNT_I,
	TRAD_ACCT_I,
	THA_ACCT_TYPE_C,
	THA_ACCT_STUS_C,
	PALL_BUSN_UNIT_I,
	PALL_DEPT_I,
	BALN_A,
	IACR_MTD_A,
	IACR_FYTD_A,
	DALY_AGGR_FEE_A,
	BALN_D,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I
	
	 FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.THA_ACCT
	;

	-- - SEMANTIC INFORMATION COULD NOT BE LOADED FOR PVTECH.UTIL_BTCH_ISAC. CHECK IF THE NAME IS INVALID OR DUPLICATED. **

CREATE OR REPLACE VIEW PVTECH.UTIL_BTCH_ISAC
	(
	      BTCH_KEY_I,
	      BTCH_RQST_S,
	      BTCH_RUN_D,
	      SRCE_SYST_M,
	      BTCH_STUS_C,
	      STUS_CHNG_S
	)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	
	BTCH_KEY_I,
	BTCH_RQST_S,
	SRCE_SYST_M,
	BTCH_STUS_C,
	STUS_CHNG_S
	
FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.UTIL_BTCH_ISAC
	;

	-- - SEMANTIC INFORMATION COULD NOT BE LOADED FOR PVTECH.UTIL_PARM. CHECK IF THE NAME IS INVALID OR DUPLICATED. **

CREATE OR REPLACE VIEW PVTECH.UTIL_PARM
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*
	FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.UTIL_PARM;

CREATE OR REPLACE VIEW PVTECH.UTIL_PROS_ISAC
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*
	FROM
	NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.UTIL_PROS_ISAC;

	-- - SEMANTIC INFORMATION COULD NOT BE LOADED FOR CALBASICS. CHECK IF THE NAME IS INVALID OR DUPLICATED. **

	-- - MISSING DEPENDENT OBJECT "Sys_Calendar.CALENDARTMP" **
	-- - SEMANTIC INFORMATION COULD NOT BE LOADED FOR Sys_Calendar.CALENDAR. CHECK IF THE NAME IS INVALID OR DUPLICATED. **

-- CREATE OR REPLACE VIEW Sys_Calendar.CALENDAR
-- 	(
-- 	  calendar_date,
-- 	  day_of_week,
-- 	  day_of_month,
-- 	  day_of_year,
-- 	  day_of_calendar,
-- 	  weekday_of_month,
-- 	  week_of_month,
-- 	  week_of_year,
-- 	  week_of_calendar,
-- 	  month_of_quarter,
-- 	  month_of_year,
-- 	  month_of_calendar,
-- 	  quarter_of_year,
-- 	  quarter_of_calendar,
-- 	  year_of_calendar)
-- 	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
-- 	AS
-- 	SELECT
-- 	calendar_date,
-- 	PUBLIC.TD_DAY_OF_WEEK_UDF(calendar_date),
-- 	DAYOFMONTH(calendar_date),
-- 	DAYOFYEAR(calendar_date),
-- 	DAYOFYEAR(calendar_date), -- DayNumber_Of_Calendar FUNCTION NOT SUPPORTED ***/!!!
--     TRUNC(DATEDIFF(DAY,DATE_TRUNC('MONTH',calendar_date),calendar_date)/7) -- DayOccurrence_Of_Month FUNCTION NOT SUPPORTED ***/!!!
-- 	DayOccurrence_Of_Month(calendar_date),
-- 	PUBLIC.WEEKNUMBER_OF_MONTH_UDF(calendar_date),
-- 	PUBLIC.TD_WEEK_OF_YEAR_UDF(calendar_date),
-- 	PUBLIC.TD_WEEK_OF_YEAR_UDF(calendar_date), -- WeekNumber_Of_Calendar FUNCTION NOT SUPPORTED ***/!!!
-- 	WeekNumber_Of_Calendar(calendar_date),
-- 	QUARTER(calendar_date), -- MonthNumber_Of_Quarter FUNCTION NOT SUPPORTED ***/!!!
-- 	MonthNumber_Of_Quarter(calendar_date),
-- 	MonthNumber_Of_Year(calendar_date), -- MonthNumber_Of_Year FUNCTION NOT SUPPORTED ***/!!!
-- 	MonthNumber_Of_Year(calendar_date),
-- 	month_of_calendar,
-- 	QUARTER(calendar_date),
-- 	quarter_of_calendar,
-- 	YEAR(calendar_date)
-- FROM Sys_Calendar.CALENDARTMP;

CREATE OR REPLACE VIEW Sys_Calendar.CALENDAR
(
  calendar_date,
  day_of_week,
  day_of_month,
  day_of_year,
  day_of_calendar,
  weekday_of_month,
  week_of_month,
  week_of_year,
  week_of_calendar,
  month_of_quarter,
  month_of_year,
  month_of_calendar,
  quarter_of_year,
  quarter_of_calendar,
  year_of_calendar
)
COMMENT = '{ "origin": "sf_sc", "name": "custom_calendar_dimension", "version": {  "major": 1,  "minor": 0,  "patch": "0.0" }, "attributes": {  "component": "snowflake",  "convertedOn": "07/24/2025",  "domain": "snowflake" }}'
AS
SELECT
  -- Calendar Date
  DATEADD(day, SEQ4(), '1901-01-01'::DATE) AS calendar_date,
  -- Day of Week (1=Sunday, 7=Saturday)
  DAYOFWEEK(calendar_date) AS day_of_week,
  -- Day of Month
  DAYOFMONTH(calendar_date) AS day_of_month,
  -- Day of Year
  DAYOFYEAR(calendar_date) AS day_of_year,
  -- Day of Calendar (using Day of Week for this interpretation)
  DAYOFWEEK(calendar_date) AS day_of_calendar,
  -- Weekday of Month (e.g., 1st Monday, 2nd Tuesday - calculated as week number within the month)
  CEIL(DAYOFMONTH(calendar_date) / 7) AS weekday_of_month,
  -- Week of Month (similar to weekday_of_month, representing the week number within the month)
  CEIL(DAYOFMONTH(calendar_date) / 7) AS week_of_month,
  -- Week of Year
  WEEKOFYEAR(calendar_date) AS week_of_year,
  -- Week of Calendar (using Week of Year for this interpretation)
  WEEKOFYEAR(calendar_date) AS week_of_calendar,
  -- Month of Quarter (1-3)
  MONTH(calendar_date) - ((QUARTER(calendar_date) - 1) * 3) AS month_of_quarter,
  -- Month of Year (1-12)
  MONTH(calendar_date) AS month_of_year,
  -- Month of Calendar (using Month of Year for this interpretation)
  MONTH(calendar_date) AS month_of_calendar,
  -- Quarter of Year (1-4)
  QUARTER(calendar_date) AS quarter_of_year,
  -- Quarter of Calendar (using Quarter of Year for this interpretation)
  QUARTER(calendar_date) AS quarter_of_calendar,
  -- Year of Calendar
  YEAR(calendar_date) AS year_of_calendar
FROM
  -- TABLE(GENERATOR(ROWCOUNT => (DATEDIFF(day, '1901-01-01'::DATE, '2050-12-31'::DATE) + 1)));
  TABLE(GENERATOR(ROWCOUNT => 54789));

	-- - SEMANTIC INFORMATION COULD NOT BE LOADED FOR CALENDARTMP. CHECK IF THE NAME IS INVALID OR DUPLICATED. **

CREATE OR REPLACE VIEW sys_calendar.CALENDARTMP
	(
	  calendar_date,
	  day_of_week,
	  day_of_month,
	  day_of_year,
	  day_of_calendar,
	  weekday_of_month,
	  week_of_month,
	  week_of_year,
	  week_of_calendar,
	  month_of_quarter,
	  month_of_year,
	  month_of_calendar,
	  quarter_of_year,
	  quarter_of_calendar,
	  year_of_calendar)
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	calendar_date,
	mod(
	(day_of_calendar + 0), 7) + 1,
	day_of_month,
	day_of_year,
	day_of_calendar,
	(day_of_month - 1) / 7 + 1,
	(day_of_month - mod( (day_of_calendar + 0), 7) + 6) / 7,
	(day_of_year - mod( (day_of_calendar + 0), 7) + 6) / 7,
	(day_of_calendar - mod( (day_of_calendar + 0), 7) + 6) / 7,
	mod(
	(month_of_year - 1), 3) + 1,
	month_of_year,
	month_of_year + 12 * year_of_calendar,
	(month_of_year + 2) / 3,
	(month_of_year + 2) / 3 + 4 * year_of_calendar,
	year_of_calendar + 1900
FROM
	sys_calendar.CALBASICS;

/* 

-- OUT OF SCOPE

CREATE OR REPLACE VIEW SYSLIB.DBSDataRelatedErrors
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	ErrorCode FROM TABLE (DBSDataRelatedErrors_TBF() ) as t1;
 */
 /* 

-- OUT OF SCOPE

CREATE OR REPLACE VIEW SYSLIB.DBSRetryableErrors
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	SELECT
	ErrorCode FROM TABLE (DBSRetryableErrors_TBF() ) as t1;

*/
-- SEMANTIC INFORMATION COULD NOT BE LOADED FOR CALENDARTMP. CHECK IF THE NAME IS INVALID OR DUPLICATED.

-- CREATE OR REPLACE VIEW CALENDARTMP
-- (
--    calendar_date,
--    day_of_week,
--    day_of_month,
--    day_of_year,
--    day_of_calendar,
--    weekday_of_month,
--    week_of_month,
--    week_of_year,
--    week_of_calendar,
--    month_of_quarter,
--    month_of_year,
--    month_of_calendar,
--    quarter_of_year,
--    quarter_of_calendar,
--    year_of_calendar)
-- COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
-- AS
-- SELECT
--  calendar_date,
--  mod(
--  (day_of_calendar + 0), 7) + 1,
--  day_of_month,
--  day_of_year,
--  day_of_calendar,
--  (day_of_month - 1) / 7 + 1,
--  (day_of_month - mod( (day_of_calendar + 0), 7) + 6) / 7,
--  (day_of_year - mod( (day_of_calendar + 0), 7) + 6) / 7,
--  (day_of_calendar - mod( (day_of_calendar + 0), 7) + 6) / 7,
--  mod(
--  (month_of_year - 1), 3) + 1,
--  month_of_year,
--  month_of_year + 12 * year_of_calendar,
--  (month_of_year + 2) / 3,
--  (month_of_year + 2) / 3 + 4 * year_of_calendar,
--  year_of_calendar + 1900
-- FROM
--  sys_calendar.CALBASICS;

CREATE OR REPLACE VIEW PVTECH.UTIL_BTCH_ISAC
 (
       BTCH_KEY_I,
       BTCH_RQST_S,
       BTCH_RUN_D,
       SRCE_SYST_M,
       BTCH_STUS_C,
       STUS_CHNG_S
 )
 COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
 AS
 SELECT
 
	BTCH_KEY_I,
	BTCH_RQST_S,
	SRCE_SYST_M,
	BTCH_STUS_C,
	STUS_CHNG_S
	
FROM
 NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.UTIL_BTCH_ISAC
 ;

CREATE OR REPLACE VIEW PVTECH.UTIL_PARM
 COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
 AS
 -- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE.
 SELECT
 *
 FROM
 NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.UTIL_PARM;

