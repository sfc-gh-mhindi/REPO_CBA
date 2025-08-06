{{ config(materialized='view', tags=['XfmHL_APP_PRODFrmExt']) }}

WITH XfmBusinessRules__OutApptPdctAcctDS AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.SRCE_CHAR_1_C)) THEN (InXfmBusinessRules.SRCE_CHAR_1_C) ELSE ""))) = 0) Then DEFAULT_NULL_VALUE ELSE InXfmBusinessRules.TARG_CHAR_C,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.SRCE_CHAR_1_C IS NOT NULL, {{ ref('ModNullHandling') }}.SRCE_CHAR_1_C, ''))) = 0, DEFAULT_NULL_VALUE, {{ ref('ModNullHandling') }}.TARG_CHAR_C) AS svTargCharC,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.HL_REPAYMENT_PERIOD_CAT_ID)) THEN (InXfmBusinessRules.HL_REPAYMENT_PERIOD_CAT_ID) ELSE ""))) = 0) Then DEFAULT_NULL_VALUE ELSE InXfmBusinessRules.PAYT_FREQ_C,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.HL_REPAYMENT_PERIOD_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.HL_REPAYMENT_PERIOD_CAT_ID, ''))) = 0, DEFAULT_NULL_VALUE, {{ ref('ModNullHandling') }}.PAYT_FREQ_C) AS svPaytFreqC,
		-- *SRC*: \(20)If svTargCharC = '999007' then 'RPR2107' else  if svErrorCode = DEFAULT_NULL_VALUE then 'REJ2015' else '',
		IFF(svTargCharC = '999007', 'RPR2107', IFF(svErrorCode = DEFAULT_NULL_VALUE, 'REJ2015', '')) AS svErrorTargCharC,
		-- *SRC*: \(20)If svPaytFreqC = '9' then 'RPR2108' else  if svPaytFreqC = DEFAULT_NULL_VALUE then 'REJ2014' else '',
		IFF(svPaytFreqC = '9', 'RPR2108', IFF(svPaytFreqC = DEFAULT_NULL_VALUE, 'REJ2014', '')) AS svErrorPaytFreqC,
		-- *SRC*: \(20)if (Len(Trim(( IF IsNotNull((InXfmBusinessRules.AMOUNT)) THEN (InXfmBusinessRules.AMOUNT) ELSE ""))) = 0) then 'N' else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.AMOUNT IS NOT NULL, {{ ref('ModNullHandling') }}.AMOUNT, ''))) = 0, 'N', 'Y') AS svLoadApptPdctAmt1,
		-- *SRC*: \(20)if (Len(Trim(( IF IsNotNull((InXfmBusinessRules.TOTAL_LOAN_AMOUNT)) THEN (InXfmBusinessRules.TOTAL_LOAN_AMOUNT) ELSE ""))) = 0) then 'N' else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.TOTAL_LOAN_AMOUNT IS NOT NULL, {{ ref('ModNullHandling') }}.TOTAL_LOAN_AMOUNT, ''))) = 0, 'N', 'Y') AS svLoadApptPdctAmt2,
		-- *SRC*: \(20)if (Len(Trim(( IF IsNotNull((InXfmBusinessRules.GDW_UPDATED_LDP_PAID_ON_AMOUNT)) THEN (InXfmBusinessRules.GDW_UPDATED_LDP_PAID_ON_AMOUNT) ELSE ""))) = 0) then 'N' else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.GDW_UPDATED_LDP_PAID_ON_AMOUNT IS NOT NULL, {{ ref('ModNullHandling') }}.GDW_UPDATED_LDP_PAID_ON_AMOUNT, ''))) = 0, 'N', 'Y') AS svLoadApptPdctAmt3,
		-- *SRC*: \(20)if (Len(Trim(( IF IsNotNull((InXfmBusinessRules.HL_REPAYMENT_PERIOD_CAT_ID)) THEN (InXfmBusinessRules.HL_REPAYMENT_PERIOD_CAT_ID) ELSE ""))) = 0) then 'N' else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.HL_REPAYMENT_PERIOD_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.HL_REPAYMENT_PERIOD_CAT_ID, ''))) = 0, 'N', 'Y') AS svLoadApptPdctRpay,
		-- *SRC*: \(20)if (Len(Trim(( IF IsNotNull((InXfmBusinessRules.LOAN_TERM_MONTHS)) THEN (InXfmBusinessRules.LOAN_TERM_MONTHS) ELSE ""))) = 0) then 'N' else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.LOAN_TERM_MONTHS IS NOT NULL, {{ ref('ModNullHandling') }}.LOAN_TERM_MONTHS, ''))) = 0, 'N', 'Y') AS svLoadApptPdctFeat,
		-- *SRC*: \(20)if Len(Trim(( IF IsNotNull((InXfmBusinessRules.ACCOUNT_NUMBER)) THEN (InXfmBusinessRules.ACCOUNT_NUMBER) ELSE ""))) = 0 Then 'N' else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.ACCOUNT_NUMBER IS NOT NULL, {{ ref('ModNullHandling') }}.ACCOUNT_NUMBER, ''))) = 0, 'N', 'Y') AS svLoadApptPdctAcct,
		-- *SRC*: \(20)If svErrorTargCharC <> '' OR svErrorPaytFreqC <> '' Then @TRUE Else @FALSE,
		IFF(svErrorTargCharC <> '' OR svErrorPaytFreqC <> '', @TRUE, @FALSE) AS svRejectFlag,
		-- *SRC*: \(20)If svErrorTargCharC <> '' Then svErrorTargCharC Else  if svErrorPaytFreqC <> '' Then svErrorPaytFreqC Else '',
		IFF(svErrorTargCharC <> '', svErrorTargCharC, IFF(svErrorPaytFreqC <> '', svErrorPaytFreqC, '')) AS svErrorCode,
		-- *SRC*: \(20)If (svLoadApptPdctRpay = 'Y' And svPaytFreqC = '9') Then 'RPR2108' Else  If (svLoadApptPdctFeat = 'Y' And svTargCharC = '999007') Then 'RPR2107' Else '',
		IFF(svLoadApptPdctRpay = 'Y' AND svPaytFreqC = '9', 'RPR2108', IFF(svLoadApptPdctFeat = 'Y' AND svTargCharC = '999007', 'RPR2107', '')) AS ErrorCode,
		-- *SRC*: \(20)If ErrorCode <> "" Then @TRUE Else @FALSE,
		IFF(ErrorCode <> '', @TRUE, @FALSE) AS RejectFlag,
		-- *SRC*: "CSEHL" : InXfmBusinessRules.HL_APP_PROD_ID,
		CONCAT('CSEHL', {{ ref('ModNullHandling') }}.HL_APP_PROD_ID) AS APPT_PDCT_I,
		-- *SRC*: \(20)if len(trim(InXfmBusinessRules.ACCOUNT_NUMBER)) = 9 then "HLSHL" : InXfmBusinessRules.ACCOUNT_NUMBER else 'SAPSPAU06' : InXfmBusinessRules.ACCOUNT_NUMBER,
		IFF(LEN(TRIM({{ ref('ModNullHandling') }}.ACCOUNT_NUMBER)) = 9, CONCAT('HLSHL', {{ ref('ModNullHandling') }}.ACCOUNT_NUMBER), CONCAT('SAPSPAU06', {{ ref('ModNullHandling') }}.ACCOUNT_NUMBER)) AS ACCT_I,
		'LOAN' AS REL_TYPE_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate('99991231', '%yyyy%mm%dd'),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		RUN_STREAM AS RUN_STRM
	FROM {{ ref('ModNullHandling') }}
	WHERE svLoadApptPdctAcct = 'Y'
)

SELECT * FROM XfmBusinessRules__OutApptPdctAcctDS