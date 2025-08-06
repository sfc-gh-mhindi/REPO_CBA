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
