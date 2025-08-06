{{ config(materialized='view', tags=['XfmAppt_Pdct_AcctFrmTmp']) }}

WITH Funnel_Update AS (
	SELECT
		EROR_ACCT_NUMB as EROR_ACCT_NUMB,
		PROS_KEY_EFFT_I as PROS_KEY_EFFT_I,
		EXPY_DATE as EXPY_DATE,
		PROS_KEY_EXPY_I as PROS_KEY_EXPY_I
	FROM {{ ref('Const_Acct_I__IdnnError_AccNum_U') }}
	UNION ALL
	SELECT
		EROR_ACCT_NUMB,
		PROS_KEY_EFFT_I,
		EXPY_DATE,
		PROS_KEY_EXPY_I
	FROM {{ ref('Const_Acct_I__IdnnError_RepAccNum_U') }}
)

SELECT * FROM Funnel_Update