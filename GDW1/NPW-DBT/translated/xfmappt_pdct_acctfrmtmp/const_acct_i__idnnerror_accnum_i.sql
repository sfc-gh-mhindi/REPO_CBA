{{ config(materialized='view', tags=['XfmAppt_Pdct_AcctFrmTmp']) }}

WITH Const_Acct_I__IdnnError_AccNum_I AS (
	SELECT
		-- *SRC*: \(20)If IsNull(Trans.ACCT_I) Then 'Y' Else  If Trans.ACCT_I = '' Then 'Y' Else 'N',
		IFF({{ ref('Funnel_Acct_I') }}.ACCT_I IS NULL, 'Y', IFF({{ ref('Funnel_Acct_I') }}.ACCT_I = '', 'Y', 'N')) AS svNotNullAcctI,
		-- *SRC*: DateFromDaysSince(-1, StringToDate(pRUN_STRM_PROS_D, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(pRUN_STRM_PROS_D, '%yyyy%mm%dd')) AS svExpyD,
		{{ ref('Funnel_Acct_I') }}.ACCOUNT_NUMBER AS EROR_ACCT_NUMB,
		'CSEPO001' AS CONV_M,
		'APPT_PDCT_ACCT' AS EROR_TABL_NAME,
		'ACCT_I' AS EROR_COLM_NAME,
		'Cannot find the ACCT_I in LOCN_SERV' AS RJCT_REAS,
		-- *SRC*: StringToTimestamp(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTOTIMESTAMP(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS LOAD_S,
		pGDW_PROS_ID_1 AS PROS_KEY_EFFT_I,
		'9999-12-31' AS EXPY_DATE,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		'0' AS ROW_SECU_ACCS_C,
		-- *SRC*: Trans.APPT_PDCT_I : "|" : Trans.ACCOUNT_NUMBER : "|" : ( IF IsNotNull((Trans.REL_TYPE_C)) THEN (Trans.REL_TYPE_C) ELSE ("<null>")) : "|" : ( IF IsNotNull((Trans.EFFT_D)) THEN (Trans.EFFT_D) ELSE ("<nul>")) : "|" : ( IF IsNotNull((Trans.EXPY_D)) THEN (Trans.EXPY_D) ELSE ("<null>")) : "|" : pGDW_PROS_ID_1 : "|" : ( IF IsNotNull((Trans.PROS_KEY_EXPY_I)) THEN (Trans.PROS_KEY_EXPY_I) ELSE ("<null>")) : "|" : ( IF IsNotNull((Trans.EROR_SEQN_I)) THEN (Trans.EROR_SEQN_I) ELSE ("<null>")),
		CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT({{ ref('Funnel_Acct_I') }}.APPT_PDCT_I, '|'), {{ ref('Funnel_Acct_I') }}.ACCOUNT_NUMBER), '|'), IFF({{ ref('Funnel_Acct_I') }}.REL_TYPE_C IS NOT NULL, {{ ref('Funnel_Acct_I') }}.REL_TYPE_C, '<null>')), '|'), IFF({{ ref('Funnel_Acct_I') }}.EFFT_D IS NOT NULL, {{ ref('Funnel_Acct_I') }}.EFFT_D, '<nul>')), '|'), IFF({{ ref('Funnel_Acct_I') }}.EXPY_D IS NOT NULL, {{ ref('Funnel_Acct_I') }}.EXPY_D, '<null>')), '|'), pGDW_PROS_ID_1), '|'), IFF({{ ref('Funnel_Acct_I') }}.PROS_KEY_EXPY_I IS NOT NULL, {{ ref('Funnel_Acct_I') }}.PROS_KEY_EXPY_I, '<null>')), '|'), IFF({{ ref('Funnel_Acct_I') }}.EROR_SEQN_I IS NOT NULL, {{ ref('Funnel_Acct_I') }}.EROR_SEQN_I, '<null>')) AS RJCT_RECD
	FROM {{ ref('Funnel_Acct_I') }}
	WHERE svNotNullAcctI = 'Y' AND {{ ref('Funnel_Acct_I') }}.ERR_FLG = '0' AND {{ ref('Funnel_Acct_I') }}.REL_TYPE_C = 'OVDR '
)

SELECT * FROM Const_Acct_I__IdnnError_AccNum_I