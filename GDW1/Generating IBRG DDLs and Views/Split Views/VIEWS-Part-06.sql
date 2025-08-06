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

