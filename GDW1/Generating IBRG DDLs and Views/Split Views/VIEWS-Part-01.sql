USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE GDW1;




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
	"rule_code",
	"rule_step_seqn",
	"prty",
	"vald_from",
	"vald_to",
	"rule_desn",
	"rule_step_desn",
	"lkup1_text",
	"lkup1_numb",
	"lkup1_date",
	"lkup1_add_attr",
	"lkup2_text",
	"lkup2_numb",
	"lkup2_date",
	"lkup2_add_attr",
	"rule_cmmt",
	"updt_dtts",
	"crat_dtts"
	 FROM
	GDW1_IBRG."pdcbods"."ods_rule"
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
	"username",
	"row_secu_prfl_c",
	"my_service_no",
	"req_no",
	"ritm_no",
	"sar_no",
	"cmmt",
	"updt_username",
	"updt_date",
	"updt_dtts"
FROM
	GDW1_IBRG."pdsecurity"."row_levl_secu_user_prfl"
	;



CREATE OR REPLACE VIEW PVDATA.GDW_EFFT_DATE
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."starcadproddata"."gdw_efft_date";



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
	"int_grup_i",
	"dept_i",
	"dept_role_c",
	"srce_syst_c",
	"vald_from_d",
	"vald_to_d",
	"efft_d",
	"expy_d",
	"pros_key_efft_i",
	"pros_key_expy_i",
	"row_secu_accs_c"
FROM
	gdw1_ibrg."starcadproddata"."int_grup_dept"
WHERE
(
/* Start - RLS */
	COALESCE("row_secu_accs_c",0) = 0 OR GETBIT( (
	SELECT
             try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
FROM
             GDW1.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
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
             GDW1.PVDATA.GDW_EFFT_DATE
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
	"int_grup_i",
    "paty_i",
    "efft_d",
    "expy_d",
    "srce_syst_c",
    "pros_key_efft_i",
    "pros_key_expy_i",
    "eror_seqn_i",
    "prim_clnt_f",
    "rel_i",
    "srce_syst_paty_int_grup_i",
    "orig_srce_syst_paty_type_c",
    "orig_srce_syst_paty_i",
    "rel_c",
    "vald_from_d",
    "vald_to_d",
    "row_secu_accs_c",
    "prim_clnt_slct_c"
FROM
	GDW1_IBRG."starcadproddata"."paty_int_grup"
WHERE (
-- /* Start - RLS */
-- 	COALESCE(row_secu_accs_c,0) = 0 OR GETBIT( (
-- 	SELECT
--              row_secu_prfl_c
-- FROM
--              GDW1.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
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
             GDW1.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
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
             gdw1.PVDATA.GDW_EFFT_DATE
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
	GDW1_IBRG."pddstg"."acct_paty_dedup";

CREATE OR REPLACE VIEW PVDSTG.ACCT_PATY_REL_THA
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."pddstg"."acct_paty_rel_tha";

CREATE OR REPLACE VIEW PVDSTG.ACCT_PATY_REL_WSS
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."pddstg"."acct_paty_rel_wss";

CREATE OR REPLACE VIEW PVDSTG.ACCT_PATY_THA_NEW_RNGE
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."pddstg"."acct_paty_tha_new_rnge";

CREATE OR REPLACE VIEW PVDSTG.ACCT_REL_WSS_DITPS
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."pddstg"."acct_rel_wss_ditps";

CREATE OR REPLACE VIEW PVDSTG.DERV_ACCT_PATY_ADD
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."pddstg"."derv_acct_paty_add";

CREATE OR REPLACE VIEW PVDSTG.DERV_ACCT_PATY_CHG
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."pddstg"."derv_acct_paty_chg";

CREATE OR REPLACE VIEW PVDSTG.DERV_ACCT_PATY_CURR
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."pddstg"."derv_acct_paty_curr";

CREATE OR REPLACE VIEW PVDSTG.DERV_ACCT_PATY_DEL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."pddstg"."derv_acct_paty_del";

CREATE OR REPLACE VIEW PVDSTG.DERV_ACCT_PATY_FLAG
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."pddstg"."derv_acct_paty_flag";

CREATE OR REPLACE VIEW PVDSTG.DERV_ACCT_PATY_NON_RM
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."pddstg"."derv_acct_paty_non_rm";

CREATE OR REPLACE VIEW PVDSTG.DERV_ACCT_PATY_RM
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."pddstg"."derv_acct_paty_rm";

CREATE OR REPLACE VIEW PVDSTG.DERV_ACCT_PATY_ROW_SECU_FIX
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."pddstg"."derv_acct_paty_row_secu_fix";

CREATE OR REPLACE VIEW PVDSTG.DERV_PRTF_ACCT_PATY_PSST
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."pddstg"."derv_prtf_acct_paty_psst";

CREATE OR REPLACE VIEW PVDSTG.DERV_PRTF_ACCT_PATY_STAG
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."pddstg"."derv_prtf_acct_paty_stag";

CREATE OR REPLACE VIEW PVDSTG.DERV_PRTF_ACCT_STAG
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."pddstg"."derv_prtf_acct_stag";

