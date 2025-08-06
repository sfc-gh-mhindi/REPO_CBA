{{ config(materialized='view', tags=['XfmCclAppProdFrmExt3']) }}

WITH Funnel_Data AS (
	SELECT
		APPT_PDCT_I as APPT_PDCT_I,
		ACCT_I as ACCT_I,
		REL_TYPE_C as REL_TYPE_C,
		EFFT_D as EFFT_D,
		EXPY_D as EXPY_D,
		PROS_KEY_EFFT_I as PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I as PROS_KEY_EXPY_I,
		EROR_SEQN_I as EROR_SEQN_I,
		RUN_STRM as RUN_STRM
	FROM {{ ref('XfmBusinessRules__ApptPdctAcct1') }}
	UNION ALL
	SELECT
		APPT_PDCT_I,
		ACCT_I,
		REL_TYPE_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		RUN_STRM
	FROM {{ ref('XfmBusinessRules__ApptPdctAcct2') }}
)

SELECT * FROM Funnel_Data