{{ config(materialized='view', tags=['XfmAppt_Empl']) }}

WITH Funnel AS (
	SELECT
		APPT_I as APPT_I,
		EMPL_I as EMPL_I,
		EMPL_ROLE_C as EMPL_ROLE_C,
		EFFT_D as EFFT_D,
		EXPY_D as EXPY_D,
		PROS_KEY_EFFT_I as PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I as PROS_KEY_EXPY_I,
		EROR_SEQN_I as EROR_SEQN_I,
		RUN_STRM as RUN_STRM
	FROM {{ ref('Xfm__Xfm_to_Tgt_own') }}
	UNION ALL
	SELECT
		APPT_I,
		EMPL_I,
		EMPL_ROLE_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		RUN_STRM
	FROM {{ ref('Xfm__Xfm_to_Tgt') }}
)

SELECT * FROM Funnel