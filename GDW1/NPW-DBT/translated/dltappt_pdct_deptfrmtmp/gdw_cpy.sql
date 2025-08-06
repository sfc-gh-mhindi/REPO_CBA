{{ config(materialized='view', tags=['DltAppt_Pdct_DeptFrmTMP']) }}

WITH gdw_cpy AS (
	SELECT
		APPT_PDCT_I,
		DEPT_I,
		DEPT_ROLE_C,
		SRCE_SYST_C,
		BRCH_N,
		EFFT_D
	FROM {{ ref('Trans') }}
)

SELECT * FROM gdw_cpy