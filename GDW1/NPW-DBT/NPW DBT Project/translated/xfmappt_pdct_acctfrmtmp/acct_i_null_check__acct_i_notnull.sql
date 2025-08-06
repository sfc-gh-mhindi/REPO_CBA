{{ config(materialized='view', tags=['XfmAppt_Pdct_AcctFrmTmp']) }}

WITH Acct_I_Null_Check__Acct_I_NotNull AS (
	SELECT
		APPT_PDCT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS REPAYMENT_ACCOUNT_NUMBER,
		-- *SRC*: SetNull(),
		SETNULL() AS ACCOUNT_NUMBER,
		ACCT_I,
		REL_TYPE_C,
		EFFT_D,
		PROS_KEY_EFFT_I,
		ERR_FLG,
		PROS_KEY_EXPY_I,
		EXPY_D,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('Temp_Acct_Pdct_Acct') }}
	WHERE {{ ref('Temp_Acct_Pdct_Acct') }}.ACCT_I IS NOT NULL
)

SELECT * FROM Acct_I_Null_Check__Acct_I_NotNull