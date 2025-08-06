{{ config(materialized='view', tags=['XfmFaPropClntFrmExt']) }}

WITH XfmBusinessRules__OutErrorMapPdctN AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.FA_ENTITY_CAT_ID)) THEN (InXfmBusinessRules.FA_ENTITY_CAT_ID) ELSE ""))) = 0) Then DEFAULT_NULL_VALUE ELSE InXfmBusinessRules.PATY_TYPE_C,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.FA_ENTITY_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.FA_ENTITY_CAT_ID, ''))) = 0, DEFAULT_NULL_VALUE, {{ ref('ModNullHandling') }}.PATY_TYPE_C) AS svPatyTypeC,
		-- *SRC*: \(20)If svPatyTypeC = '9' then 'RPR6200' else '',
		IFF(svPatyTypeC = '9', 'RPR6200', '') AS svErrorCode,
		-- *SRC*: \(20)If svErrorCode <> "" Then @TRUE Else @FALSE,
		IFF(svErrorCode <> '', @TRUE, @FALSE) AS svRejectFlag,
		2 AS DeleteChangeCode,
		{{ ref('ModNullHandling') }}.FA_PROPOSED_CLIENT_ID AS SRCE_KEY_I,
		'PATY_TYPE_C' AS CONV_M,
		'LOOKUP' AS CONV_MAP_RULE_M,
		'MAP_CSE_COIN_PATY_TYPE' AS TRSF_TABL_M,
		{{ ref('ModNullHandling') }}.PATY_TYPE_C AS VALU_CHNG_BFOR_X,
		svPatyTypeC AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name = ' : DSJobName,
		CONCAT('Job Name = ', DSJobName) AS TRSF_X,
		'PATY_TYPE_C' AS TRSF_COLM_M
	FROM {{ ref('ModNullHandling') }}
	WHERE svErrorCode = 'RPR6200' AND {{ ref('ModNullHandling') }}.change_code <> DeleteChangeCode
)

SELECT * FROM XfmBusinessRules__OutErrorMapPdctN