{{ config(materialized='view', tags=['XfmHL_APP_PROD_PURPOSEFrmExt']) }}

WITH XfmBusinessRules__OutTmpAppProdPurpDS AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.HL_LOAN_PURPOSE_CAT_ID)) THEN (InXfmBusinessRules.HL_LOAN_PURPOSE_CAT_ID) ELSE ""))) = 0) Then DEFAULT_NULL_VALUE ELSE InXfmBusinessRules.PURP_TYPE_C,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.HL_LOAN_PURPOSE_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.HL_LOAN_PURPOSE_CAT_ID, ''))) = 0, DEFAULT_NULL_VALUE, {{ ref('ModNullHandling') }}.PURP_TYPE_C) AS svApptPurp,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.HL_LOAN_PURPOSE_CAT_ID)) THEN (InXfmBusinessRules.HL_LOAN_PURPOSE_CAT_ID) ELSE ""))) = 0) Then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.HL_LOAN_PURPOSE_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.HL_LOAN_PURPOSE_CAT_ID, ''))) = 0, 'N', 'Y') AS svLoadApptPdctPurp,
		-- *SRC*: \(20)If svApptPurp = '9999' then 'RPR2107' else  if svApptPurp = DEFAULT_NULL_VALUE then 'REJ2013' else '',
		IFF(svApptPurp = '9999', 'RPR2107', IFF(svApptPurp = DEFAULT_NULL_VALUE, 'REJ2013', '')) AS svErrorCode,
		-- *SRC*: \(20)If svErrorCode <> "" Then @TRUE Else @FALSE,
		IFF(svErrorCode <> '', @TRUE, @FALSE) AS svRejectFlag,
		-- *SRC*: 'CSEHL' : InXfmBusinessRules.HL_APP_PROD_ID,
		CONCAT('CSEHL', {{ ref('ModNullHandling') }}.HL_APP_PROD_ID) AS APPT_PDCT_I,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		{{ ref('ModNullHandling') }}.HL_APP_PROD_PURPOSE_ID AS SRCE_SYST_APPT_PDCT_PURP_I,
		PURP_TYPE_C,
		-- *SRC*: SetNull(),
		SETNULL() AS PURP_CLAS_C,
		'CSE' AS SRCE_SYST_C,
		{{ ref('ModNullHandling') }}.AMOUNT AS PURP_A,
		'AUD' AS CNCY_C,
		{{ ref('ModNullHandling') }}.MAIN_PURPOSE AS MAIN_PURP_F,
		-- *SRC*: StringToDate('99991231', '%yyyy%mm%dd'),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		RUN_STREAM AS RUN_STRM
	FROM {{ ref('ModNullHandling') }}
	WHERE svLoadApptPdctPurp = 'Y'
)

SELECT * FROM XfmBusinessRules__OutTmpAppProdPurpDS