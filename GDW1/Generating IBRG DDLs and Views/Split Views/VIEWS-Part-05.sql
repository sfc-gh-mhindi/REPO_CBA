
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

