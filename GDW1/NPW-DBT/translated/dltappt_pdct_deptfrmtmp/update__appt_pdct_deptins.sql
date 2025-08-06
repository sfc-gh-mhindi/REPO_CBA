{{ config(materialized='view', tags=['DltAppt_Pdct_DeptFrmTMP']) }}

WITH Update__Appt_Pdct_DeptIns AS (
	SELECT
		APPT_PDCT_I,
		DEPT_I,
		DEPT_ROLE_C,
		SRCE_SYST_C,
		BRCH_N,
		EFFT_D,
		EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		change_code
	FROM {{ ref('Join') }}
	WHERE {{ ref('Join') }}.change_code = 1 OR {{ ref('Join') }}.change_code = 3
)

SELECT * FROM Update__Appt_Pdct_DeptIns