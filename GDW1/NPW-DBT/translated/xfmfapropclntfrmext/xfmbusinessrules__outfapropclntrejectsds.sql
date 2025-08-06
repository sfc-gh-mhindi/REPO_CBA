{{ config(materialized='view', tags=['XfmFaPropClntFrmExt']) }}

WITH XfmBusinessRules__OutFAPropClntRejectsDS AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.FA_ENTITY_CAT_ID)) THEN (InXfmBusinessRules.FA_ENTITY_CAT_ID) ELSE ""))) = 0) Then DEFAULT_NULL_VALUE ELSE InXfmBusinessRules.PATY_TYPE_C,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.FA_ENTITY_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.FA_ENTITY_CAT_ID, ''))) = 0, DEFAULT_NULL_VALUE, {{ ref('ModNullHandling') }}.PATY_TYPE_C) AS svPatyTypeC,
		-- *SRC*: \(20)If svPatyTypeC = '9' then 'RPR6200' else '',
		IFF(svPatyTypeC = '9', 'RPR6200', '') AS svErrorCode,
		-- *SRC*: \(20)If svErrorCode <> "" Then @TRUE Else @FALSE,
		IFF(svErrorCode <> '', @TRUE, @FALSE) AS svRejectFlag,
		2 AS DeleteChangeCode,
		FA_PROPOSED_CLIENT_ID,
		COIN_ENTITY_ID,
		CLIENT_CORRELATION_ID,
		COIN_ENTITY_NAME,
		FA_ENTITY_CAT_ID,
		FA_UNDERTAKING_ID,
		FA_PROPOSED_CLIENT_CAT_ID,
		ETL_PROCESS_DT AS ETL_D,
		ORIG_ETL_D,
		svErrorCode AS EROR_C
	FROM {{ ref('ModNullHandling') }}
	WHERE svRejectFlag AND {{ ref('ModNullHandling') }}.change_code <> DeleteChangeCode
)

SELECT * FROM XfmBusinessRules__OutFAPropClntRejectsDS