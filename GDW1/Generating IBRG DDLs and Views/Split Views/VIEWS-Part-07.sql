
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

