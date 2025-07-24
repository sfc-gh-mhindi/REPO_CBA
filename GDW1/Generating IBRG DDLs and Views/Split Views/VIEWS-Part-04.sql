
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

