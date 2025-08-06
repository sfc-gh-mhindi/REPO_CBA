{{ config(materialized='view', tags=['XfmHL_FEATURE_ATTRFrmExt']) }}

WITH XfmBusinessRules__InTmpApptPdctAmtDS3 AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.HL_FEATURE_CAT_ID)) THEN (InXfmBusinessRules.HL_FEATURE_CAT_ID) ELSE ""))) = 0) Then DEFAULT_NULL_VALUE ELSE InXfmBusinessRules.TARG_CHAR_C_LKP1,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.HL_FEATURE_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.HL_FEATURE_CAT_ID, ''))) = 0, DEFAULT_NULL_VALUE, {{ ref('ModNullHandling') }}.TARG_CHAR_C_LKP1) AS svTargChar1C,
		{{ ref('ModNullHandling') }}.TARG_CHAR_C_LKP2 AS svTargChar2C,
		-- *SRC*: \(20)If IsValid('date', StringToDate(InXfmBusinessRules.HL_FEATURE_DATE, '%yyyy%mm%dd')) Then 'N' Else 'Y',
		IFF(ISVALID('date', STRINGTODATE({{ ref('ModNullHandling') }}.HL_FEATURE_DATE, '%yyyy%mm%dd')), 'N', 'Y') AS svErrorDate,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.HL_FEATURE_SPEC_REPAY)) THEN (InXfmBusinessRules.HL_FEATURE_SPEC_REPAY) ELSE ""))) = 0 Or InXfmBusinessRules.HL_FEATURE_CAT_ID <> '14' Then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.HL_FEATURE_SPEC_REPAY IS NOT NULL, {{ ref('ModNullHandling') }}.HL_FEATURE_SPEC_REPAY, ''))) = 0 OR {{ ref('ModNullHandling') }}.HL_FEATURE_CAT_ID <> '14', 'N', 'Y') AS svLoadFeatAmt2,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.HL_FEATURE_EST_INT_AMT)) THEN (InXfmBusinessRules.HL_FEATURE_EST_INT_AMT) ELSE ""))) = 0 Or InXfmBusinessRules.HL_FEATURE_CAT_ID <> '14' Then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.HL_FEATURE_EST_INT_AMT IS NOT NULL, {{ ref('ModNullHandling') }}.HL_FEATURE_EST_INT_AMT, ''))) = 0 OR {{ ref('ModNullHandling') }}.HL_FEATURE_CAT_ID <> '14', 'N', 'Y') AS svLoadFeatAmt3,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.HL_FEATURE_FEE)) THEN (InXfmBusinessRules.HL_FEATURE_FEE) ELSE ""))) = 0 Or InXfmBusinessRules.HL_FEATURE_CAT_ID <> '14' Then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.HL_FEATURE_FEE IS NOT NULL, {{ ref('ModNullHandling') }}.HL_FEATURE_FEE, ''))) = 0 OR {{ ref('ModNullHandling') }}.HL_FEATURE_CAT_ID <> '14', 'N', 'Y') AS svLoadApptPdctFeat2,
		-- *SRC*: \(20)If svTargChar1C = '9999' then 'RPR4002' else '',
		IFF(svTargChar1C = '9999', 'RPR4002', '') AS svErrorCode1,
		-- *SRC*: \(20)If svTargChar2C = '9999' then 'RPR4003' else '',
		IFF(svTargChar2C = '9999', 'RPR4003', '') AS svErrorCode2,
		-- *SRC*: \(20)if svErrorCode1 <> '' then svErrorCode1 else  if svErrorCode2 <> '' then svErrorCode2 else '',
		IFF(svErrorCode1 <> '', svErrorCode1, IFF(svErrorCode2 <> '', svErrorCode2, '')) AS svErrorCode,
		-- *SRC*: \(20)If svErrorCode1 <> '' or svErrorCode2 <> '' Then @TRUE Else @FALSE,
		IFF(svErrorCode1 <> '' OR svErrorCode2 <> '', @TRUE, @FALSE) AS svRejectFlag,
		-- *SRC*: 'CSEHL' : InXfmBusinessRules.HL_APP_PROD_ID,
		CONCAT('CSEHL', {{ ref('ModNullHandling') }}.HL_APP_PROD_ID) AS APPT_PDCT_I,
		'FTEI' AS AMT_TYPE_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate('99991231', '%yyyy%mm%dd'),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		'AUD' AS CNCY_C,
		{{ ref('ModNullHandling') }}.HL_FEATURE_EST_INT_AMT AS APPT_PDCT_A,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		RUN_STREAM AS RUN_STRM,
		'2' AS DLTA_VERS
	FROM {{ ref('ModNullHandling') }}
	WHERE svLoadFeatAmt3 = 'Y'
)

SELECT * FROM XfmBusinessRules__InTmpApptPdctAmtDS3