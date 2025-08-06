{{ config(materialized='view', tags=['XfmAppt_Pdct_AcctFrmTmp']) }}

WITH Const_Acct_I__IdnnError_RepAccNum_U AS (
	SELECT
		-- *SRC*: \(20)If IsNull(Trans.ACCT_I) Then 'Y' Else  If Trans.ACCT_I = '' Then 'Y' Else 'N',
		IFF({{ ref('Funnel_Acct_I') }}.ACCT_I IS NULL, 'Y', IFF({{ ref('Funnel_Acct_I') }}.ACCT_I = '', 'Y', 'N')) AS svNotNullAcctI,
		-- *SRC*: DateFromDaysSince(-1, StringToDate(pRUN_STRM_PROS_D, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(pRUN_STRM_PROS_D, '%yyyy%mm%dd')) AS svExpyD,
		{{ ref('Funnel_Acct_I') }}.REPAYMENT_ACCOUNT_NUMBER AS EROR_ACCT_NUMB,
		PROS_KEY_EFFT_I,
		-- *SRC*: DateFromDaysSince(-1, StringToDate(pRUN_STRM_PROS_D, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(pRUN_STRM_PROS_D, '%yyyy%mm%dd')) AS EXPY_DATE,
		pGDW_PROS_ID_1 AS PROS_KEY_EXPY_I
	FROM {{ ref('Funnel_Acct_I') }}
	WHERE svNotNullAcctI = 'N' AND {{ ref('Funnel_Acct_I') }}.ERR_FLG = '1' AND {{ ref('Funnel_Acct_I') }}.REL_TYPE_C = 'RPAY '
)

SELECT * FROM Const_Acct_I__IdnnError_RepAccNum_U