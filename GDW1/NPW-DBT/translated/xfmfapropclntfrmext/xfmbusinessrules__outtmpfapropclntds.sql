{{ config(materialized='view', tags=['XfmFaPropClntFrmExt']) }}

WITH XfmBusinessRules__OutTmpFAPropClntDS AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.FA_ENTITY_CAT_ID)) THEN (InXfmBusinessRules.FA_ENTITY_CAT_ID) ELSE ""))) = 0) Then DEFAULT_NULL_VALUE ELSE InXfmBusinessRules.PATY_TYPE_C,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.FA_ENTITY_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.FA_ENTITY_CAT_ID, ''))) = 0, DEFAULT_NULL_VALUE, {{ ref('ModNullHandling') }}.PATY_TYPE_C) AS svPatyTypeC,
		-- *SRC*: \(20)If svPatyTypeC = '9' then 'RPR6200' else '',
		IFF(svPatyTypeC = '9', 'RPR6200', '') AS svErrorCode,
		-- *SRC*: \(20)If svErrorCode <> "" Then @TRUE Else @FALSE,
		IFF(svErrorCode <> '', @TRUE, @FALSE) AS svRejectFlag,
		2 AS DeleteChangeCode,
		{{ ref('ModNullHandling') }}.FA_PROPOSED_CLIENT_ID AS FA_PROP_CLNT_ID,
		{{ ref('ModNullHandling') }}.COIN_ENTITY_ID AS COIN_ENTY_ID,
		{{ ref('ModNullHandling') }}.CLIENT_CORRELATION_ID AS CLNT_CORL_ID,
		{{ ref('ModNullHandling') }}.COIN_ENTITY_NAME AS COIN_ENTY_NAME,
		{{ ref('ModNullHandling') }}.FA_ENTITY_CAT_ID AS FA_ENTY_CAT_ID,
		{{ ref('ModNullHandling') }}.FA_UNDERTAKING_ID AS FA_UTAK_ID,
		{{ ref('ModNullHandling') }}.FA_PROPOSED_CLIENT_CAT_ID AS FA_PROP_CLNT_CAT_ID,
		ORIG_ETL_D,
		{{ ref('ModNullHandling') }}.change_code AS CHNG_CODE,
		-- *SRC*: "CSE" : "C1" : InXfmBusinessRules.FA_UNDERTAKING_ID,
		CONCAT(CONCAT('CSE', 'C1'), {{ ref('ModNullHandling') }}.FA_UNDERTAKING_ID) AS INT_GRUP_I,
		{{ ref('ModNullHandling') }}.FA_PROPOSED_CLIENT_ID AS SRCE_SYST_PATY_I,
		{{ ref('ModNullHandling') }}.COIN_ENTITY_ID AS ORIG_SRCE_SYST_PATY_I,
		{{ ref('ModNullHandling') }}.COIN_ENTITY_NAME AS UNID_PATY_M,
		-- *SRC*: \(20)If (svPatyTypeC = DEFAULT_NULL_VALUE) then '' else svPatyTypeC,
		IFF(svPatyTypeC = DEFAULT_NULL_VALUE, '', svPatyTypeC) AS PATY_TYPE_C,
		'CSE' AS SRCE_SYST_C
	FROM {{ ref('ModNullHandling') }}
	WHERE 
)

SELECT * FROM XfmBusinessRules__OutTmpFAPropClntDS