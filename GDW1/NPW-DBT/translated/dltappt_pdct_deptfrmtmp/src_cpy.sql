{{ config(materialized='view', tags=['DltAppt_Pdct_DeptFrmTMP']) }}

WITH src_cpy AS (
	SELECT
		APPT_PDCT_I,
		DEPT_I,
		DEPT_ROLE_C,
		SRCE_SYST_C,
		BRCH_N,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		RUN_STRM
	FROM {{ ref('XfmApptPdctDept') }}
)

SELECT * FROM src_cpy