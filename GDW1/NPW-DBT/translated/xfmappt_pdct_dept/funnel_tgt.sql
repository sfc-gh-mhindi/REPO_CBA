{{ config(materialized='view', tags=['XfmAppt_Pdct_Dept']) }}

WITH Funnel_Tgt AS (
	SELECT
		APPT_PDCT_I as APPT_PDCT_I,
		DEPT_I as DEPT_I,
		DEPT_ROLE_C as DEPT_ROLE_C,
		SRCE_SYST_C as SRCE_SYST_C,
		BRCH_N as BRCH_N,
		EFFT_D as EFFT_D,
		EXPY_D as EXPY_D,
		PROS_KEY_EFFT_I as PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I as PROS_KEY_EXPY_I,
		RUN_STRM as RUN_STRM
	FROM {{ ref('Xfm_Logn') }}
	UNION ALL
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
	FROM {{ ref('Xfm_Nom') }}
)

SELECT * FROM Funnel_Tgt