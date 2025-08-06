USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE GDW1;



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