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

CREATE OR REPLACE VIEW PVDSTG.DERV_PRTF_PATY_STAG
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."pddstg"."derv_prtf_paty_stag";

CREATE OR REPLACE VIEW PVDSTG.GRD_GNRC_MAP_BUSN_SEGM_PRTY
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."pddstg"."grd_gnrc_map_busn_segm_prty";

CREATE OR REPLACE VIEW PVDSTG.GRD_GNRC_MAP_DERV_PATY_HOLD
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."pddstg"."grd_gnrc_map_derv_paty_hold";

CREATE OR REPLACE VIEW PVDSTG.GRD_GNRC_MAP_DERV_PATY_REL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."pddstg"."grd_gnrc_map_derv_paty_rel";

CREATE OR REPLACE VIEW PVDSTG.GRD_GNRC_MAP_DERV_UNID_PATY
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."pddstg"."grd_gnrc_map_derv_unid_paty";

CREATE OR REPLACE VIEW PVDSTG.GRD_GNRC_MAP_PATY_HOLD_PRTY
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."pddstg"."grd_gnrc_map_paty_hold_prty";

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
	"pros_key_i",
	"conv_m",
	"conv_type_m",
	"pros_rqst_s",
	"pros_last_rqst_s",
	"pros_rqst_q",
	"btch_run_d",
	"btch_key_i",
	"srce_syst_m",
	"srce_m",
	"trgt_m",
	"succ_f",
	"comt_f",
	"comt_s",
	"mlti_load_efft_d",
	"syst_s",
	"mlti_load_comt_s",
	"syst_et_q",
	"syst_uv_q",
	"syst_ins_q",
	"syst_upd_q",
	"syst_del_q",
	"syst_et_tabl_m",
	"syst_uv_tabl_m",
	"syst_head_et_tabl_m",
	"syst_head_uv_tabl_m",
	"syst_trlr_et_tabl_m",
	"syst_trlr_uv_tabl_m",
	"prev_pros_key_i",
	"head_recd_type_c",
	"head_file_m",
	"head_btch_run_d",
	"head_file_crat_s",
	"head_genr_prgm_m",
	"head_btch_key_i",
	"head_pros_key_i",
	"head_pros_prev_key_i",
	"trlr_recd_type_c",
	"trlr_recd_q",
	"trlr_hash_totl_a",
	"trlr_colm_hash_totl_m",
	"trlr_eror_recd_q",
	"trlr_file_comt_s",
	"trlr_recd_isrt_q",
	"trlr_recd_updt_q",
	"trlr_recd_delt_q"
FROM
	GDW1_IBRG."pdpaty"."util_pros_isac"
	;

CREATE OR REPLACE VIEW PVTECH.ACCT_APPT_PDCT
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."starcadproddata"."acct_appt_pdct";

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
	"paty_i",
	"acct_i",
	"paty_acct_rel_c",
	"rel_levl_c",
	"rel_reas_c",
	"rel_stus_c",
	"srce_syst_c",
	"efft_d",
	"expy_d",
	"pros_key_efft_i",
	"pros_key_expy_i",
	"row_secu_accs_c"
FROM (
	SELECT
             "paty_i",
             "acct_i",
             "paty_acct_rel_c",
             "rel_levl_c",
             "rel_reas_c",
             "rel_stus_c",
             "srce_syst_c",
             "efft_d",
             "expy_d",
             "pros_key_efft_i",
             "pros_key_expy_i",
             CASE
				 WHEN "acct_i" ILIKE 'FMS%'
					                        THEN 1 ELSE "row_secu_accs_c"
             END AS "row_secu_accs_c"
       FROM
             GDW1_IBRG."pdpaty"."acct_paty"
	) ACCT_PATY
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
	"subj_acct_i",
	"objc_acct_i",
	"rel_c",
	"efft_d",
	"expy_d",
	"strt_d",
	"rel_expy_d",
	"pros_key_efft_i",
	"pros_key_expy_i",
	"eror_seqn_i",
	"row_secu_accs_c",
	"rel_stus_c",
	"srce_syst_c",
	"subj_acct_levl_n",
	"objc_acct_levl_n",
	"crin_shre_p",
	"dr_int_shre_p",
	"record_deleted_flag",
	"ctl_id",
	"process_name",
	"process_id",
	"update_process_name",
	"update_process_id"
FROM (
	SELECT
             "subj_acct_i",
             "objc_acct_i",
             "rel_c",
             "efft_d",
             "expy_d",
             "strt_d",
             "rel_expy_d",
             "pros_key_efft_i",
             "pros_key_expy_i",
             "eror_seqn_i",
             CASE
				 WHEN "subj_acct_i" ILIKE 'FMS%'
					                        THEN 1 ELSE "row_secu_accs_c"
             END AS "row_secu_accs_c",
             "rel_stus_c",
             "srce_syst_c",
             "subj_acct_levl_n",
             "objc_acct_levl_n",
             "crin_shre_p",
             "dr_int_shre_p",
             "record_deleted_flag",
             "ctl_id",
             "process_name",
             "process_id",
             "update_process_name",
             "update_process_id"
       FROM
             GDW1_IBRG."starcadproddata"."acct_rel"
	) ACCT_REL
WHERE
(
/* Start - RLS */
	COALESCE("row_secu_accs_c",0) = 0 OR GETBIT( (
	SELECT
             -- row_secu_prfl_c
             try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
FROM
             GDW1.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
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
	"acct_i",
"srce_syst_paty_i",
"srce_syst_c",
"paty_acct_rel_c",
"efft_d",
"expy_d",
"pros_key_efft_i",
"pros_key_expy_i",
"eror_seqn_i",
"row_secu_accs_c",
"orig_srce_syst_c"
FROM (
	SELECT
             "acct_i",
          "srce_syst_paty_i",
          "srce_syst_c",
          "paty_acct_rel_c",
          "efft_d",
          "expy_d",
          "pros_key_efft_i",
          "pros_key_expy_i",
          "eror_seqn_i",
             CASE
				 WHEN "acct_i" ILIKE 'FMS%'
					                        THEN 1 ELSE "row_secu_accs_c"
             END AS "row_secu_accs_c",
          "orig_srce_syst_c"
          FROM
             GDW1_IBRG."starcadproddata"."acct_unid_paty"
	) ACCT_UNID_PATY

Where (
/* Start - RLS */
	COALESCE("row_secu_accs_c",0) = 0 OR GETBIT( (
	SELECT
             -- row_secu_prfl_c
             try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
FROM
             GDW1.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
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
	GDW1_IBRG."starcadproddata"."acct_xref_bps_cbs";

CREATE OR REPLACE VIEW PVTECH.ACCT_XREF_MAS_DAR
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*
	 FROM
	GDW1_IBRG."starcadproddata"."acct_xref_mas_dar";

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
	"appt_i",
	"appt_c",
	"appt_form_c",
	"appt_qlfy_c",
	"stus_trak_i",
	"appt_orig_c",
	"appt_n",
	"srce_syst_c",
	"srce_syst_appt_i",
	"appt_crat_d",
	"rate_seek_f",
	"pros_key_efft_i",
	"eror_seqn_i",
	"orig_appt_srce_c",
	"rel_mgr_stat_c",
	"appt_recv_s",
	"appt_recv_d",
	"appt_recv_t",
	"appt_entr_poit_m",
	"ext_to_pdct_syst_d",
	"efft_d"
FROM
	GDW1_IBRG."starcadproddata"."appt"
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
	GDW1_IBRG."starcadproddata"."appt_dept"
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
	GDW1_IBRG."starcadproddata"."appt_pdct"
	;

CREATE OR REPLACE VIEW PVTECH.APPT_PDCT_FEAT
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."starcadproddata"."appt_pdct_feat";

CREATE OR REPLACE VIEW PVTECH.APPT_PDCT_REL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."starcadproddata"."appt_pdct_rel";

CREATE OR REPLACE VIEW PVTECH.APPT_REL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."starcadproddata"."appt_rel";

CREATE OR REPLACE VIEW PVTECH.APPT_TRNF_DETL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."starcadproddata"."appt_trnf_detl";

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
	"evnt_i",
	"srce_syst_evnt_i",
	"evnt_actl_d",
	"srce_syst_c",
	"pros_key_efft_i",
	"eror_seqn_i",
	"srce_syst_evnt_type_i",
	"evnt_actl_t",
	"row_secu_accs_c",
	"evnt_actv_type_c",
	"efft_d",
	"expy_d",
	"record_deleted_flag",
	"ctl_id",
	"process_name",
	"process_id",
	"update_process_name",
	"update_process_id"
FROM
	GDW1_IBRG."starcadproddata"."busn_evnt"
WHERE

((
/* Start - RLS */
	COALESCE("row_secu_accs_c",0) = 0 OR GETBIT( (
	SELECT
             -- "row_secu_prfl_c"
             try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
FROM
             GDW1.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
WHERE
             UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
),"row_secu_accs_c"
) = 1
/* End - RLS */
));



CREATE OR REPLACE VIEW CALBASICS.CALBASICS
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
	  GDW1_IBRG."syscalendar"."caldates";


CREATE OR REPLACE VIEW PVTECH.CALENDAR
             COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
             AS
             -- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
             SELECT	*
             FROM  GDW1.CALBASICS.CALBASICS;
			 --GDW1_IBRG."syscalendar"."calendar";

CREATE OR REPLACE VIEW PVTECH.CLS_FCLY
             COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
             AS
             -- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
             SELECT
	*
	 FROM
	GDW1_IBRG."starcadproddata"."cls_fcly";

CREATE OR REPLACE VIEW PVTECH.CLS_UNID_PATY
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*
	 FROM
	GDW1_IBRG."starcadproddata"."cls_unid_paty";

CREATE OR REPLACE VIEW PVTECH.DAR_ACCT
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*
	 FROM
	GDW1_IBRG."starcadproddata"."dar_acct";

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
	T1."dept_i",
	T1."ancs_dept_i",
	T1."ancs_levl_n",
	T1."as_at_d"
FROM
	GDW1_IBRG."pdgrd"."dept_dimn_node_ancs_curr" T1;

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
	"acct_i",
	"paty_i",
	"assc_acct_i",
	"paty_acct_rel_c",
	"prfr_paty_f",
	"srce_syst_c",
	"efft_d",
	"expy_d",
	"pros_key_efft_i",
	"pros_key_expy_i",
	"row_secu_accs_c"

FROM (
	SELECT
             "acct_i",
             "paty_i",
             "assc_acct_i",
             "paty_acct_rel_c",
             "prfr_paty_f",
             "srce_syst_c",
             "efft_d",
             "expy_d",
             "pros_key_efft_i",
             "pros_key_expy_i",
             CASE
				 WHEN "acct_i" ILIKE 'FMS%'
					                        THEN 1 ELSE "row_secu_accs_c"
             END AS "row_secu_accs_c"

            FROM
             GDW1_IBRG."starcadproddata"."derv_acct_paty"
	) DERV_ACCT_PATY
WHERE
	       (
/* Start - RLS */
	COALESCE("row_secu_accs_c",0) = 0 OR GETBIT( (
	SELECT
             -- "row_secu_prfl_c"
             try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
FROM
             GDW1.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
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
	"acct_i",
"int_grup_i",
"derv_prtf_catg_c",
"derv_prtf_clas_c",
"derv_prtf_type_c",
"vald_from_d",
"vald_to_d",
"efft_d",
"expy_d",
"ptcl_n",
"rel_mnge_i",
"prtf_code_x",
"srce_syst_c",
"row_secu_accs_c"
FROM
	GDW1_IBRG."starcadproddata"."derv_prtf_acct_rel"

WHERE (
(
/* Start - RLS */
	COALESCE("row_secu_accs_c",0) = 0 OR GETBIT( (
	SELECT
             -- "row_secu_prfl_c"
             try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
FROM
             GDW1.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
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
	"int_grup_i",
"derv_prtf_type_c",
"derv_prtf_catg_c",
"derv_prtf_clas_c",
"vald_from_d",
"vald_to_d",
"efft_d",
"expy_d",
"ptcl_n",
"rel_mnge_i",
"prtf_code_x",
"derv_prtf_role_c",
"role_play_type_x",
"role_play_i",
"srce_syst_c",
"row_secu_accs_c"
FROM
	GDW1_IBRG."starcadproddata"."derv_prtf_own_rel"

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
	"paty_i",
"int_grup_i",
"derv_prtf_catg_c",
"derv_prtf_clas_c",
"derv_prtf_type_c",
"vald_from_d",
"vald_to_d",
"efft_d",
"expy_d",
"ptcl_n",
"rel_mnge_i",
"prtf_code_x",
"srce_syst_c",
"row_secu_accs_c"
FROM
	GDW1_IBRG."starcadproddata"."derv_prtf_paty_rel"

WHERE (
(
/* Start - RLS */
	COALESCE("row_secu_accs_c",0) = 0 OR GETBIT( (
	SELECT
             -- "row_secu_prfl_c"
             try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
FROM
             GDW1.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
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
	"evnt_i",
	"evnt_actv_type_c",
	"invt_evnt_f",
	"fncl_acct_evnt_f",
	"ctct_evnt_f",
	"busn_evnt_f",
	"pros_key_efft_i",
	"eror_seqn_i",
	"fncl_nval_evnt_f",
	"incd_f",
	"insr_evnt_f",
	"insr_nval_evnt_f",
	"row_secu_accs_c",
	"fncl_gl_evnt_f",
	"autt_autn_evnt_f",
	"coll_evnt_f",
	"srce_syst_c",
	"evnt_reas_c",
	"efft_d",
	"expy_d",
	"record_deleted_flag",
	"ctl_id",
	"process_name",
	"process_id",
	"update_process_name",
	"update_process_id"
FROM
	GDW1_IBRG."starcadproddata"."evnt"
WHERE
((
/* Start - RLS */
	COALESCE("row_secu_accs_c",0) = 0 OR GETBIT( (
	SELECT
             -- "row_secu_prfl_c"
             try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
FROM
             GDW1.PVSECURITY.ROW_LEVL_SECU_USER_PRFL 
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
	*
            FROM
	GDW1_IBRG."starcadproddata"."evnt_empl"
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
	GDW1_IBRG."starcadproddata"."evnt_int_grup";

CREATE OR REPLACE VIEW PVTECH.GRD_DEPT_FLAT_CURR
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*
from
	GDW1_IBRG."pdgrd"."grd_dept_flat_curr";

CREATE OR REPLACE VIEW PVTECH.GRD_GNRC_MAP
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	GDW1_IBRG."pdgrd"."grd_gnrc_map";

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
	"int_grup_i",
"int_grup_type_c",
"srce_syst_int_grup_i",
"srce_syst_c",
"efft_d",
"expy_d",
"pros_key_efft_i",
"pros_key_expy_i",
"eror_seqn_i",
"int_grup_m",
"orig_srce_syst_int_grup_i",
"crat_d",
"qlfy_c",
"ptcl_n",
"rel_mnge_i",
"vald_to_d",
"row_secu_accs_c",
"int_grup_catg_c",
"iso_cnty_c"
FROM
	"GDW1_IBRG". "starcadproddata"."int_grup"

Where (
/* Start - RLS */
	COALESCE("row_secu_accs_c",0) = 0 OR GETBIT( (
	SELECT
            try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
FROM
             gdw1.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
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
	"int_grup_i",
	"empl_i",
	"empl_role_c",
	"efft_d",
	"expy_d",
	"pros_key_efft_i",
	"pros_key_expy_i",
	"eror_seqn_i",
	"srce_syst_c",
	"vald_from_d",
	"vald_to_d",
	"row_secu_accs_c"
FROM
	"GDW1_IBRG". "starcadproddata"."int_grup_empl"
WHERE
(
/* Start - RLS */
	COALESCE("row_secu_accs_c",0) = 0 OR GETBIT( (
	SELECT
            try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
FROM
             gdw1.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
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
	"GDW1_IBRG". "starcadproddata"."int_grup_unid_paty";

CREATE OR REPLACE VIEW PVTECH.MAP_CMS_PDCT
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*
	FROM
	GDW1_IBRG."pdcms"."map_cms_pdct";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_ADRS_TYPE
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*  FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_adrs_type"
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_ACQR_SRCE
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_appt_acqr_srce";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_C
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_appt_c";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_CMPE
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_appt_cmpe";

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
	"hlm_appt_type_catg_i",
	"appt_c",
	"efft_d",
	"expy_d"
FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_appt_code_hm"
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_COND
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*  FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_appt_cond"
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
	"exec_docu_recv_type",
	"docu_dely_recv_c",
	"efft_d",
	"expy_d"
FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_appt_docu_dely"
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_FEAT
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_appt_feat";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_FORM
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_appt_form";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_ORIG
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_appt_orig";

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
	"pexa_flag",
"feat_valu_c",
"efft_d",
"expy_d"
FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_appt_pdct_feat"

	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_PDCT_PATY_ROLE
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_appt_pdct_paty_role";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_PURP_CL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_appt_purp_cl";


CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_PURP_CLAS_CL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_appt_purp_clas_cl";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_PURP_HL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_appt_purp_hl";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_PURP_PL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_appt_purp_pl";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_APPT_QLFY
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*  FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_appt_qlfy"
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
	"qa_question_id",
	"qstn_c",
	"efft_d",
	"expy_d"
FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_appt_qstn_hl"
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
	"qa_answer_id",
	"resp_c",
	"efft_d",
	"expy_d"
FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_appt_qstn_resp_hl"
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_CMPE_IDNN
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*  FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_cmpe_idnn"
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_CNTY
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*  FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_cnty"
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
	"cris_pdct_id",
	"acct_qlfy_c",
	"srce_syst_c",
	"efft_d",
	"expy_d"
FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_cris_pdct"
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_DOCU_METH
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*  FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_docu_meth"
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_ENV_CHLD_PATY_REL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_env_chld_paty_rel";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_ENV_EVNT_ACTV_TYPE
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_env_evnt_actv_type";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_ENV_PATY_REL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_env_paty_rel";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_ENV_PATY_TYPE
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_env_paty_type";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_FEAT_OVRD_REAS_HL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_feat_ovrd_reas_hl";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_FEAT_OVRD_REAS_HL_D
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_feat_ovrd_reas_hl_d";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_FEAT_OVRD_REAS_PL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_feat_ovrd_reas_pl";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_FEE_CAPL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_fee_capl";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_FNDD_METH
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*  FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_fndd_meth"
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
	"clp_job_family_cat_id",
	     "job_comm_catg_c",
	     "efft_d",
	     "expy_d"
	 FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_job_comm_catg"
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_LOAN_FNDD_METH
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_loan_fndd_meth";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_LOAN_TERM_PL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_loan_term_pl";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_LPC_DEPT_HL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_lpc_dept_hl";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_ORIG_APPT_SRCE
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*
	 FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_orig_appt_srce";

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
	"hl_busn_chnl_cat_i",
	"orig_appt_srce_syst_c",
	"efft_d",
	"expy_d"
FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_orig_appt_srce_hm"
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_OVRD_FEE_FRQ_CL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_ovrd_fee_frq_cl";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_PACK_PDCT_HL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_pack_pdct_hl";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_PACK_PDCT_PL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_pack_pdct_pl";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_PAYT_FREQ
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	-- GDW1_IBRG."map_cse_payt_freq"."map_cse_payt_freq";
    GDW1_IBRG."starcadproddata"."map_cse_payt_freq"; --REPLACED SCHEMA NAME BY MH

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_PDCT_REL_CL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_pdct_rel_cl";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_PL_ACQR_TYPE
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_pl_acqr_type";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_SM_CASE_STUS
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_sm_case_stus";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_SM_CASE_STUS_REAS
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_sm_case_stus_reas";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_STATE
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*  FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_state"
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_TRNF_OPTN
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_trnf_optn";

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_TU_APPT_C
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*  FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_tu_appt_c"
	;

CREATE OR REPLACE VIEW PVTECH.MAP_CSE_UNID_PATY_CATG_PL
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	* FROM
	"GDW1_IBRG". "starcadproddata"."map_cse_unid_paty_catg_pl";

CREATE OR REPLACE VIEW PVTECH.MOS_FCLY
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*  FROM
	"GDW1_IBRG". "starcadproddata"."mos_fcly";

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
	"loan_i",
"fcly_i",
"mos_appt_c",
"mos_aset_libl_c",
"mos_stus_c",
"acrl_x",
"trad_cncy_orig_a",
"trad_cncy_setl_a",
"trad_cncy_curr_baln_a",
"int_r",
"fix_int_r",
"vary_int_r",
"issu_d",
"mtur_d",
"rolv_d",
"int_payt_freq_c",
"next_int_d",
"trad_cncy_acre_a",
"last_int_payt_d",
"trad_cncy_totl_int_recv_a",
"pdct_n",
"trad_cncy_c",
"base_cncy_c",
"trad_cncy_int_a",
"aud_cncy_int_a",
"base_cncy_int_a",
"trad_cncy_avrg_baln_a",
"aud_cncy_avrg_baln_a",
"base_cncy_avrg_baln_a",
"trad_cncy_marg_a",
"aud_cncy_marg_a",
"base_cncy_marg_a",
"avrg_marg_r",
"dept_i",
"msa_baln_gl_acct_i",
"msa_int_gl_acct_i",
"efft_d",
"expy_d",
"pros_key_efft_i",
"pros_key_expy_i",
"prof_cntr_code_x"
FROM
	"GDW1_IBRG". "starcadproddata"."mos_loan";

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
	"geoa_type_c",
	"geoa_c",
	"non_work_d",
	"non_work_day_type_c",
	"non_work_day_m",
	"hldy_stus_x"
FROM
	GDW1_IBRG."pdgrd"."non_work_day"
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
	"GDW1_IBRG". "starcadproddata"."paty_appt_pdct";

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
	"paty_i",
"reld_paty_i",
"rel_i",
"rel_reas_c",
"rel_type_c",
"srce_syst_c",
"paty_role_c",
"rel_stus_c",
"rel_levl_c",
"rel_efft_d",
"rel_expy_d",
"srce_syst_rel_i",
"efft_d",
"expy_d",
"pros_key_efft_i",
"pros_key_expy_i",
"row_secu_accs_c",
"risk_aggr_f"
FROM
	GDW1_IBRG."pdpaty"."paty_rel"

Where (
/* Start - RLS */
	COALESCE("row_secu_accs_c",0) = 0 OR GETBIT( (
	SELECT
            --  "row_secu_prfl_c"
			try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
FROM
             gdw1.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
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
	"tha_acct_i",
	"acct_qlfy_c",
	"ext_d",
	"csl_clnt_i",
	"trad_acct_i",
	"tha_acct_type_c",
	"tha_acct_stus_c",
	"pall_busn_unit_i",
	"pall_dept_i",
	"baln_a",
	"iacr_mtd_a",
	"iacr_fytd_a",
	"daly_aggr_fee_a",
	"baln_d",
	"efft_d",
	"expy_d",
	"pros_key_efft_i",
	"pros_key_expy_i"
	 FROM
	"GDW1_IBRG". "starcadproddata"."tha_acct"
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
	"btch_key_i",
	"btch_rqst_s",
	"btch_run_d",
	"srce_syst_m",
	"btch_stus_c",
	"stus_chng_s"
FROM
	"GDW1_IBRG". "starcadproddata"."util_btch_isac"
	;

	-- - SEMANTIC INFORMATION COULD NOT BE LOADED FOR PVTECH.UTIL_PARM. CHECK IF THE NAME IS INVALID OR DUPLICATED. **

CREATE OR REPLACE VIEW PVTECH.UTIL_PARM
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*
	FROM
	"GDW1_IBRG". "starcadproddata"."util_parm";

CREATE OR REPLACE VIEW PVTECH.UTIL_PROS_ISAC
	COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
	AS
	-- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
	SELECT
	*
	FROM
	"GDW1_IBRG". "starcadproddata"."util_pros_isac";

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

CREATE OR REPLACE VIEW CALENDARTMP
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
	CALBASICS.CALBASICS;

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

CREATE OR REPLACE VIEW CALENDARTMP
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
 CALBASICS.CALBASICS;

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
 -- BTCH_KEY_I,
 -- BTCH_RQST_S,
 -- BTCH_RUN_D,
 -- SRCE_SYST_M,
 -- BTCH_STUS_C,
 -- STUS_CHNG_S
"btch_key_i",
"btch_rqst_s",
"btch_run_d",
"srce_syst_m",
"btch_stus_c",
"stus_chng_s"
FROM
 "GDW1_IBRG". "starcadproddata"."util_btch_isac"
 ;

CREATE OR REPLACE VIEW PVTECH.UTIL_PARM
 COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 11,  "patch": "0.0" }, "attributes": {  "component": "teradata",  "convertedOn": "07/11/2025",  "domain": "snowflake" }}'
 AS
 -- VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE.
 SELECT
 *
 FROM
 "GDW1_IBRG". "starcadproddata"."util_parm";

