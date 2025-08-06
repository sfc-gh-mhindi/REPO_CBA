{{ config(materialized='view', tags=['LdAPP_ANS_CTCT_EVNT_Ins']) }}

WITH Transformer_105__DSLink106 AS (
	SELECT
		EVNT_I,
		SRCE_SYST_EVNT_I,
		EVNT_ACTL_D,
		SRCE_SYST_C,
		CNTRL_M AS PROS_KEY_EFFT_I,
		EROR_SEQN_I,
		SRCE_SYST_EVNT_TYPE_I,
		CTCT_EVNT_TYPE_C,
		EVNT_ACTL_T,
		ROW_SECU_ACCS_C
	FROM {{ ref('TgtInsertDS') }}
	WHERE 
)

SELECT * FROM Transformer_105__DSLink106