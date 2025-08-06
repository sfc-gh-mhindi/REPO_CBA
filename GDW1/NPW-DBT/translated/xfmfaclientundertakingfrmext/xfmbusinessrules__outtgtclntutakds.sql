{{ config(materialized='view', tags=['XfmFaClientUndertakingFrmExt']) }}

WITH XfmBusinessRules__OutTgtClntUtakDS AS (
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
		-- *SRC*: 'CSEC1' : ModOut.FA_UNDERTAKING_ID,
		CONCAT('CSEC1', {{ ref('ModEvntActvTypeC') }}.FA_UNDERTAKING_ID) AS INT_GRUP_I,
		-- *SRC*: 'CSEC1' : ModOut.FA_CLIENT_UNDERTAKING_ID,
		CONCAT('CSEC1', {{ ref('ModEvntActvTypeC') }}.FA_CLIENT_UNDERTAKING_ID) AS REL_I,
		{{ ref('ModEvntActvTypeC') }}.FA_CLIENT_UNDERTAKING_ID AS SRCE_SYST_PATY_INT_GRUP_I,
		{{ ref('ModEvntActvTypeC') }}.FA_UNDERTAKING_ID AS SRCE_SYST_REL_I,
		{{ ref('ModEvntActvTypeC') }}.COIN_ENTITY_ID AS ORIG_SRCE_SYST_PATY_I,
		-- *SRC*: \(20)if (Len(trim(( IF IsNotNull((ModOut.FA_ENTITY_CAT_ID)) THEN (ModOut.FA_ENTITY_CAT_ID) ELSE ""))) = 0) then SetNull() else ModOut.PATY_TYPE_C,
		IFF(LEN(TRIM(IFF({{ ref('ModEvntActvTypeC') }}.FA_ENTITY_CAT_ID IS NOT NULL, {{ ref('ModEvntActvTypeC') }}.FA_ENTITY_CAT_ID, ''))) = 0, SETNULL(), {{ ref('ModEvntActvTypeC') }}.PATY_TYPE_C) AS ORIG_SRCE_SYST_PATY_TYPE_C,
		{{ ref('ModEvntActvTypeC') }}.IS_PRIMARY_FLAG AS PRIM_CLNT_F,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((ModOut.CIF_CODE)) THEN (ModOut.CIF_CODE) ELSE ""))) = 0 then SetNull() else ('CIFPT+' : Str('0', 10 - Len(Trim(( IF IsNotNull((ModOut.CIF_CODE)) THEN (ModOut.CIF_CODE) ELSE "")))) : Trim(( IF IsNotNull((ModOut.CIF_CODE)) THEN (ModOut.CIF_CODE) ELSE ""))),
		IFF(
	    LEN(TRIM(IFF({{ ref('ModEvntActvTypeC') }}.CIF_CODE IS NOT NULL, {{ ref('ModEvntActvTypeC') }}.CIF_CODE, ''))) = 0, SETNULL(), 
	    CONCAT(CONCAT('CIFPT+', STR('0', 10 - LEN(TRIM(IFF({{ ref('ModEvntActvTypeC') }}.CIF_CODE IS NOT NULL, {{ ref('ModEvntActvTypeC') }}.CIF_CODE, ''))))), TRIM(IFF({{ ref('ModEvntActvTypeC') }}.CIF_CODE IS NOT NULL, {{ ref('ModEvntActvTypeC') }}.CIF_CODE, '')))
	) AS PATY_I,
		'CSE' AS SRCE_SYST_C,
		'Y' AS PATY_INT_GRUP_F,
		{{ ref('ModEvntActvTypeC') }}.REL_C AS REL_TYPE_C_PRIM,
		'CUST' AS PATY_ROLE_C,
		'U' AS REL_STUS_C,
		-- *SRC*: SetNull(),
		SETNULL() AS REL_EFFT_D,
		-- *SRC*: SetNull(),
		SETNULL() AS REL_EXPY_D,
		'N/A' AS REL_REAS_C,
		'PRIM' AS REL_LEVL_C_PRIM,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((ModOut.CIF_CODE)) THEN (ModOut.CIF_CODE) ELSE ""))) = 0 then SetNull() else ('CIFPT+' : Str('0', 10 - Len(Trim(( IF IsNotNull((ModOut.CIF_CODE)) THEN (ModOut.CIF_CODE) ELSE "")))) : Trim(( IF IsNotNull((ModOut.CIF_CODE)) THEN (ModOut.CIF_CODE) ELSE ""))),
		IFF(
	    LEN(TRIM(IFF({{ ref('ModEvntActvTypeC') }}.CIF_CODE IS NOT NULL, {{ ref('ModEvntActvTypeC') }}.CIF_CODE, ''))) = 0, SETNULL(), 
	    CONCAT(CONCAT('CIFPT+', STR('0', 10 - LEN(TRIM(IFF({{ ref('ModEvntActvTypeC') }}.CIF_CODE IS NOT NULL, {{ ref('ModEvntActvTypeC') }}.CIF_CODE, ''))))), TRIM(IFF({{ ref('ModEvntActvTypeC') }}.CIF_CODE IS NOT NULL, {{ ref('ModEvntActvTypeC') }}.CIF_CODE, '')))
	) AS RELD_PATY_I_PRIM,
		svPatyRelPrimF AS PATY_REL_PRIM_F,
		{{ ref('ModEvntActvTypeC') }}.REL_C_CHILD AS REL_TYPE_C_CHLD,
		'SECN' AS REL_LEVL_C_CHLD,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((ModOut.CIF_CODE)) THEN (ModOut.CIF_CODE) ELSE ""))) = 0 then SetNull() else ('CIFPT+' : Str('0', 10 - Len(Trim(( IF IsNotNull((ModOut.CIF_CODE)) THEN (ModOut.CIF_CODE) ELSE "")))) : Trim(( IF IsNotNull((ModOut.CIF_CODE)) THEN (ModOut.CIF_CODE) ELSE ""))),
		IFF(
	    LEN(TRIM(IFF({{ ref('ModEvntActvTypeC') }}.CIF_CODE IS NOT NULL, {{ ref('ModEvntActvTypeC') }}.CIF_CODE, ''))) = 0, SETNULL(), 
	    CONCAT(CONCAT('CIFPT+', STR('0', 10 - LEN(TRIM(IFF({{ ref('ModEvntActvTypeC') }}.CIF_CODE IS NOT NULL, {{ ref('ModEvntActvTypeC') }}.CIF_CODE, ''))))), TRIM(IFF({{ ref('ModEvntActvTypeC') }}.CIF_CODE IS NOT NULL, {{ ref('ModEvntActvTypeC') }}.CIF_CODE, '')))
	) AS RELD_PATY_I_CHILD,
		svPatyRelChldF AS PATY_REL_CHLD_F,
		{{ ref('ModEvntActvTypeC') }}.FA_CLIENT_UNDERTAKING_ID AS CSE_FA_CLNT_UNTK_ID,
		{{ ref('ModEvntActvTypeC') }}.FA_UNDERTAKING_ID AS CSE_FA_UNTK_ID,
		{{ ref('ModEvntActvTypeC') }}.COIN_ENTITY_ID AS CSE_COIN_ENTY_ID,
		{{ ref('ModEvntActvTypeC') }}.CLIENT_CORRELATION_ID AS CSE_CLNT_CORR_ID,
		{{ ref('ModEvntActvTypeC') }}.FA_ENTITY_CAT_ID AS CSE_FA_ENTY_CAT_ID,
		{{ ref('ModEvntActvTypeC') }}.FA_CHILD_STATUS_CAT_ID AS CSE_FA_CHLD_STAT_CAT_ID,
		{{ ref('ModEvntActvTypeC') }}.CLIENT_RELATIONSHIP_TYPE_ID AS CSE_CLNT_REL_TYPE_ID,
		{{ ref('ModEvntActvTypeC') }}.CLIENT_POSITION AS CSE_CLNT_POSN,
		{{ ref('ModEvntActvTypeC') }}.IS_PRIMARY_FLAG AS CSE_IS_PRIM_FLAG,
		{{ ref('ModEvntActvTypeC') }}.CIF_CODE AS CSE_CIF_CODE,
		{{ ref('ModEvntActvTypeC') }}.ORIG_ETL_D AS CSE_ORIG_ETL_D
	FROM {{ ref('ModEvntActvTypeC') }}
	WHERE svCIFnullF = 'N'
)

SELECT * FROM XfmBusinessRules__OutTgtClntUtakDS