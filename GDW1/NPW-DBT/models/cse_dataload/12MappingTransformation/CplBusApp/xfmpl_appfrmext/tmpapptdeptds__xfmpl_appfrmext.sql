{{
  config(
    post_hook=[
      "INSERT overwrite INTO " ~ cvar("datasets_schema") ~ "."~ cvar("base_dir") ~"__dataset__Tmp_"~ cvar("run_stream") ~ "_APPT_DEPT__DS SELECT * FROM {{ this }}"
    ]
  )
}}

WITH xfmbusinessrules as 
(
	SELECT 
		'CSE' || 'PL' || PL_APP_ID AS APPT_I,
		'NOMN' AS DEPT_ROLE_C,
		to_date('{{ cvar("etl_process_dt") }}', 'YYYYMMDD') AS EFFT_D,
		NOMINATED_BRANCH_ID AS DEPT_I,
		to_date('99991231', 'YYYYMMDD') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		NULL AS PROS_KEY_EXPY_I,
		NULL AS EROR_SEQN_I,
		'{{ cvar("run_stream") }}' AS RUN_STRM
	
	FROM {{ ref('xfmbusinessrules__xfmpl_appfrmext') }}
	
	WHERE svLoadApptDept = 'Y'
)

SELECT
  APPT_I,
  DEPT_ROLE_C,
  EFFT_D,
  DEPT_I,
  EXPY_D,
  PROS_KEY_EFFT_I,
  PROS_KEY_EXPY_I,
  EROR_SEQN_I,
  RUN_STRM
from xfmbusinessrules

