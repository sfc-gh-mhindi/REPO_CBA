{{ config(materialized='view', tags=['LdAPP_ANS_EVNT_Ins']) }}

WITH Transformer_105__DSLink106 AS (
	SELECT
		EVNT_I,
		EVNT_ACTV_TYPE_C,
		INVT_EVNT_F,
		FNCL_ACCT_EVNT_F,
		CTCT_EVNT_F,
		BUSN_EVNT_F,
		CNTRL_M AS PROS_KEY_EFFT_I,
		EROR_SEQN_I,
		FNCL_NVAL_EVNT_F,
		INCD_F,
		INSR_EVNT_F,
		INSR_NVAL_EVNT_F,
		ROW_SECU_ACCS_C
	FROM {{ ref('TgtInsertDS') }}
	WHERE 
)

SELECT * FROM Transformer_105__DSLink106