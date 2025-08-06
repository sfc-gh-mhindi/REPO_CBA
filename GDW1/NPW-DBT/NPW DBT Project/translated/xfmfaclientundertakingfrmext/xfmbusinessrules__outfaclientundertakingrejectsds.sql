{{ config(materialized='view', tags=['XfmFaClientUndertakingFrmExt']) }}

WITH XfmBusinessRules__OutFAclientUndertakingRejectsDS AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((ModOut.CIF_CODE)) THEN (ModOut.CIF_CODE) ELSE ""))) = 0) then 'Y' else 'N',
		IFF(LEN(TRIM(IFF({{ ref('ModEvntActvTypeC') }}.CIF_CODE IS NOT NULL, {{ ref('ModEvntActvTypeC') }}.CIF_CODE, ''))) = 0, 'Y', 'N') AS svCIFnullF,
		-- *SRC*: \(20)If (trim(( IF IsNotNull((ModOut.IS_PRIMARY_FLAG)) THEN (ModOut.IS_PRIMARY_FLAG) ELSE "")) = 'Y') then 'N' else 'Y',
		IFF(TRIM(IFF({{ ref('ModEvntActvTypeC') }}.IS_PRIMARY_FLAG IS NOT NULL, {{ ref('ModEvntActvTypeC') }}.IS_PRIMARY_FLAG, '')) = 'Y', 'N', 'Y') AS svPatyRelPrimF,
		-- *SRC*: \(20)If ((Len(Trim(( IF IsNotNull((ModOut.FA_CHILD_STATUS_CAT_ID)) THEN (ModOut.FA_CHILD_STATUS_CAT_ID) ELSE ""))) = 0) or (trim(( IF IsNotNull((ModOut.IS_PRIMARY_FLAG)) THEN (ModOut.IS_PRIMARY_FLAG) ELSE "")) = 'Y')) then 'N' else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModEvntActvTypeC') }}.FA_CHILD_STATUS_CAT_ID IS NOT NULL, {{ ref('ModEvntActvTypeC') }}.FA_CHILD_STATUS_CAT_ID, ''))) = 0 OR TRIM(IFF({{ ref('ModEvntActvTypeC') }}.IS_PRIMARY_FLAG IS NOT NULL, {{ ref('ModEvntActvTypeC') }}.IS_PRIMARY_FLAG, '')) = 'Y', 'N', 'Y') AS svPatyRelChldF,
		-- *SRC*: \(20)if (Len(trim(( IF IsNotNull((ModOut.FA_ENTITY_CAT_ID)) THEN (ModOut.FA_ENTITY_CAT_ID) ELSE ""))) = 0) then 'N' else  if ModOut.PATY_TYPE_C[1, 1] = '9' then 'Y' else 'N',
		IFF(LEN(TRIM(IFF({{ ref('ModEvntActvTypeC') }}.FA_ENTITY_CAT_ID IS NOT NULL, {{ ref('ModEvntActvTypeC') }}.FA_ENTITY_CAT_ID, ''))) = 0, 'N', IFF(SUBSTRING({{ ref('ModEvntActvTypeC') }}.PATY_TYPE_C, 1, 1) = '9', 'Y', 'N')) AS svErrorPatyTypeC,
		-- *SRC*: \(20)If svPatyRelPrimF = 'N' then 'N' else  if ModOut.REL_C[1, 5] = '99999' then 'Y' else 'N',
		IFF(svPatyRelPrimF = 'N', 'N', IFF(SUBSTRING({{ ref('ModEvntActvTypeC') }}.REL_C, 1, 5) = '99999', 'Y', 'N')) AS svErrorRelCPrim,
		-- *SRC*: \(20)If svPatyRelChldF = 'N' then 'N' else  if ModOut.REL_C_CHILD[1, 5] = '99999' then 'Y' else 'N',
		IFF(svPatyRelChldF = 'N', 'N', IFF(SUBSTRING({{ ref('ModEvntActvTypeC') }}.REL_C_CHILD, 1, 5) = '99999', 'Y', 'N')) AS svErrorRelCChild,
		-- *SRC*: \(20)If svErrorPatyTypeC = 'Y' then 'RPR6203' else  if svErrorRelCPrim = 'Y' then 'RPR6204' else  if svErrorRelCChild = 'Y' then 'RPR6205' else '',
		IFF(svErrorPatyTypeC = 'Y', 'RPR6203', IFF(svErrorRelCPrim = 'Y', 'RPR6204', IFF(svErrorRelCChild = 'Y', 'RPR6205', ''))) AS svErrorCode,
		FA_CLIENT_UNDERTAKING_ID,
		FA_UNDERTAKING_ID,
		COIN_ENTITY_ID,
		CLIENT_CORRELATION_ID,
		FA_ENTITY_CAT_ID,
		FA_CHILD_STATUS_CAT_ID,
		CLIENT_RELATIONSHIP_TYPE_ID,
		CLIENT_POSITION,
		IS_PRIMARY_FLAG,
		CIF_CODE,
		ETL_PROCESS_DT AS ETL_D,
		ORIG_ETL_D,
		svErrorCode AS EROR_C
	FROM {{ ref('ModEvntActvTypeC') }}
	WHERE svErrorCode <> '' AND svCIFnullF = 'N'
)

SELECT * FROM XfmBusinessRules__OutFAclientUndertakingRejectsDS