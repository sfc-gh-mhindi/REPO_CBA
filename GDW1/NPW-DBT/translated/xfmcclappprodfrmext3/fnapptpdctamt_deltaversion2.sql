{{ config(materialized='view', tags=['XfmCclAppProdFrmExt3']) }}

WITH FnApptPdctAmt_DeltaVersion2 AS (
	SELECT
		APPT_PDCT_I as APPT_PDCT_I,
		AMT_TYPE_C as AMT_TYPE_C,
		EFFT_D as EFFT_D,
		EXPY_D as EXPY_D,
		CNCY_C as CNCY_C,
		APPT_PDCT_A as APPT_PDCT_A,
		PROS_KEY_EFFT_I as PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I as PROS_KEY_EXPY_I,
		EROR_SEQN_I as EROR_SEQN_I,
		RUN_STRM as RUN_STRM,
		DLTA_VERS as DLTA_VERS
	FROM {{ ref('XfmBusinessRules__ApptPdctAmt1') }}
	UNION ALL
	SELECT
		APPT_PDCT_I,
		AMT_TYPE_C,
		EFFT_D,
		EXPY_D,
		CNCY_C,
		APPT_PDCT_A,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		RUN_STRM,
		DLTA_VERS
	FROM {{ ref('XfmBusinessRules__ApptPdctAmt3') }}
	UNION ALL
	SELECT
		APPT_PDCT_I,
		AMT_TYPE_C,
		EFFT_D,
		EXPY_D,
		CNCY_C,
		APPT_PDCT_A,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		RUN_STRM,
		DLTA_VERS
	FROM {{ ref('XfmBusinessRules__ApptPdctAmt4') }}
)

SELECT * FROM FnApptPdctAmt_DeltaVersion2