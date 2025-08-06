{{ config(materialized='view', tags=['XfmCclAppProdFrmExt3']) }}

WITH FnApptPdctRel AS (
	SELECT
		APPT_PDCT_I as APPT_PDCT_I,
		RELD_APPT_PDCT_I as RELD_APPT_PDCT_I,
		EFFT_D as EFFT_D,
		REL_TYPE_C as REL_TYPE_C,
		EXPY_D as EXPY_D,
		PROS_KEY_EFFT_I as PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I as PROS_KEY_EXPY_I,
		EROR_SEQN_I as EROR_SEQN_I,
		RUN_STRM as RUN_STRM
	FROM {{ ref('XfmBusinessRules__ApptPdctRel1') }}
	UNION ALL
	SELECT
		APPT_PDCT_I,
		RELD_APPT_PDCT_I,
		EFFT_D,
		REL_TYPE_C,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		RUN_STRM
	FROM {{ ref('XfmBusinessRules__ApptPdctRel2') }}
)

SELECT * FROM FnApptPdctRel