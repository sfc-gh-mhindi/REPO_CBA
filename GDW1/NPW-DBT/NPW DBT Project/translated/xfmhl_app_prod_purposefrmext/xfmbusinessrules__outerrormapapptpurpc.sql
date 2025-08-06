{{ config(materialized='view', tags=['XfmHL_APP_PROD_PURPOSEFrmExt']) }}

WITH XfmBusinessRules__OutErrorMapApptPurpC AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.HL_LOAN_PURPOSE_CAT_ID)) THEN (InXfmBusinessRules.HL_LOAN_PURPOSE_CAT_ID) ELSE ""))) = 0) Then DEFAULT_NULL_VALUE ELSE InXfmBusinessRules.PURP_TYPE_C,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.HL_LOAN_PURPOSE_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.HL_LOAN_PURPOSE_CAT_ID, ''))) = 0, DEFAULT_NULL_VALUE, {{ ref('ModNullHandling') }}.PURP_TYPE_C) AS svApptPurp,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.HL_LOAN_PURPOSE_CAT_ID)) THEN (InXfmBusinessRules.HL_LOAN_PURPOSE_CAT_ID) ELSE ""))) = 0) Then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.HL_LOAN_PURPOSE_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.HL_LOAN_PURPOSE_CAT_ID, ''))) = 0, 'N', 'Y') AS svLoadApptPdctPurp,
		-- *SRC*: \(20)If svApptPurp = '9999' then 'RPR2107' else  if svApptPurp = DEFAULT_NULL_VALUE then 'REJ2013' else '',
		IFF(svApptPurp = '9999', 'RPR2107', IFF(svApptPurp = DEFAULT_NULL_VALUE, 'REJ2013', '')) AS svErrorCode,
		-- *SRC*: \(20)If svErrorCode <> "" Then @TRUE Else @FALSE,
		IFF(svErrorCode <> '', @TRUE, @FALSE) AS svRejectFlag,
		{{ ref('ModNullHandling') }}.HL_APP_PROD_PURPOSE_ID AS SRCE_KEY_I,
		'HL_LOAN_PURPOSE_CAT_ID' AS CONV_M,
		'LOOKUP' AS CONV_MAP_RULE_M,
		'MAP_CSE_APPT_PURP_HL' AS TRSF_TABL_M,
		{{ ref('ModNullHandling') }}.PURP_TYPE_C AS VALU_CHNG_BFOR_X,
		svApptPurp AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name = ' : DSJobName,
		CONCAT('Job Name = ', DSJobName) AS TRSF_X,
		'PURP_TYPE_C' AS TRSF_COLM_M
	FROM {{ ref('ModNullHandling') }}
	WHERE svErrorCode = 'RPR2107'
)

SELECT * FROM XfmBusinessRules__OutErrorMapApptPurpC