{{ config(materialized='view', tags=['XfmAppt_Pdct_AcctFrmTmp']) }}

WITH Sort AS (
	SELECT
		APPT_PDCT_I,
		ACCT_I,
		REL_TYPE_C,
		EFFT_D,
		PROS_KEY_EFFT_I,
		ERR_FLG,
		EXPY_D,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		ACCOUNT_NUMBER,
		REPAYMENT_ACCOUNT_NUMBER,
		-- *SRC*: KeyChange(),
		KEYCHANGE() AS keyChange
	FROM {{ ref('Sort_Key') }}
	ORDER BY 
)

SELECT * FROM Sort