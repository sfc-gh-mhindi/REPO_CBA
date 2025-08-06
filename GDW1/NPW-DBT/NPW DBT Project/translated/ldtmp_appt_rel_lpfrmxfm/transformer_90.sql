{{ config(materialized='view', tags=['LdTMP_APPT_REL_LPFrmXfm']) }}

WITH Transformer_90 AS (
	SELECT
		APPT_I,
		RELD_APPT_I,
		REL_TYPE_C,
		EFFT_D,
		EXPY_D,
		CNTRL_M AS PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		RUN_STRM
	FROM {{ ref('SrcApptRelDS') }}
	WHERE 
)

SELECT * FROM Transformer_90