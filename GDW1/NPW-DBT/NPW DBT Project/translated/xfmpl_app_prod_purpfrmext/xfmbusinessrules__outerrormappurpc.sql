{{ config(materialized='view', tags=['XfmPL_APP_PROD_PURPFrmExt']) }}

WITH XfmBusinessRules__OutErrorMapPurpC AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.PL_APP_PROD_PURP_CAT_ID)) THEN (InXfmBusinessRules.PL_APP_PROD_PURP_CAT_ID) ELSE ""))) = 0) Then 'UNKN' ELSE InXfmBusinessRules.PURP_TYPE_C,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.PL_APP_PROD_PURP_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.PL_APP_PROD_PURP_CAT_ID, ''))) = 0, 'UNKN', {{ ref('ModNullHandling') }}.PURP_TYPE_C) AS svApptPurp,
		-- *SRC*: \(20)If svApptPurp = '9999' then 'RPR2105' else  if svApptPurp = DEFAULT_NULL_VALUE then 'REJ2008' else '',
		IFF(svApptPurp = '9999', 'RPR2105', IFF(svApptPurp = DEFAULT_NULL_VALUE, 'REJ2008', '')) AS svErrorCode,
		-- *SRC*: \(20)If svErrorCode <> "" Then @TRUE Else @FALSE,
		IFF(svErrorCode <> '', @TRUE, @FALSE) AS svRejectFlag,
		{{ ref('ModNullHandling') }}.PL_APP_PROD_PURP_ID AS SRCE_KEY_I,
		'PL_APP_PROD_PURP_CAT_ID' AS CONV_M,
		'LOOKUP' AS CONV_MAP_RULE_M,
		'MAP_CSE_APPT_PURP_PL' AS TRSF_TABL_M,
		{{ ref('ModNullHandling') }}.PURP_TYPE_C AS VALU_CHNG_BFOR_X,
		svApptPurp AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name = ' : DSJobName,
		CONCAT('Job Name = ', DSJobName) AS TRSF_X,
		'PURP_C' AS TRSF_COLM_M
	FROM {{ ref('ModNullHandling') }}
	WHERE svErrorCode = 'RPR2105'
)

SELECT * FROM XfmBusinessRules__OutErrorMapPurpC