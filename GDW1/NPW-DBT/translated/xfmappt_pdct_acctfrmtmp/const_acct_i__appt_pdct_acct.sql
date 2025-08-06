{{ config(materialized='view', tags=['XfmAppt_Pdct_AcctFrmTmp']) }}

WITH Const_Acct_I__APPT_PDCT_ACCT AS (
	SELECT
		-- *SRC*: \(20)If IsNull(Trans.ACCT_I) Then 'Y' Else  If Trans.ACCT_I = '' Then 'Y' Else 'N',
		IFF({{ ref('Funnel_Acct_I') }}.ACCT_I IS NULL, 'Y', IFF({{ ref('Funnel_Acct_I') }}.ACCT_I = '', 'Y', 'N')) AS svNotNullAcctI,
		-- *SRC*: DateFromDaysSince(-1, StringToDate(pRUN_STRM_PROS_D, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(pRUN_STRM_PROS_D, '%yyyy%mm%dd')) AS svExpyD,
		APPT_PDCT_I,
		ACCT_I,
		REL_TYPE_C,
		EROR_SEQN_I,
		EFFT_D,
		-- *SRC*: \(20)If (Trans.keyChange) = 0 Then DateFromDaysSince(-1, StringToDate(pRUN_STRM_PROS_D, '%yyyy%mm%dd')) Else '9999-12-31',
		IFF({{ ref('Funnel_Acct_I') }}.keyChange = 0, DATEFROMDAYSSINCE(-1, STRINGTODATE(pRUN_STRM_PROS_D, '%yyyy%mm%dd')), '9999-12-31') AS EXPY_D,
		pGDW_PROS_ID_1 AS PROS_KEY_EFFT_I,
		-- *SRC*: \(20)If (Trans.keyChange) = 0 Then pGDW_PROS_ID_1 Else SetNull(),
		IFF({{ ref('Funnel_Acct_I') }}.keyChange = 0, pGDW_PROS_ID_1, SETNULL()) AS PROS_KEY_EXPY_I,
		pRUN_STRM_C AS RUN_STRM
	FROM {{ ref('Funnel_Acct_I') }}
	WHERE svNotNullAcctI = 'N'
)

SELECT * FROM Const_Acct_I__APPT_PDCT_ACCT