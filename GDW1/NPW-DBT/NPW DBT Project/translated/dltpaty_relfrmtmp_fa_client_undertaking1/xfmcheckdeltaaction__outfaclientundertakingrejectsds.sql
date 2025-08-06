{{ config(materialized='view', tags=['DltPATY_RELFrmTMP_FA_CLIENT_UNDERTAKING1']) }}

WITH XfmCheckDeltaAction__OutFAclientUndertakingRejectsDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((JoinAllFAClientUndertaking.PATY_I)) THEN (JoinAllFAClientUndertaking.PATY_I) ELSE ""))) = 0) then 'RPR6206' else '',
		IFF(LEN(TRIM(IFF({{ ref('JoinAll') }}.PATY_I IS NOT NULL, {{ ref('JoinAll') }}.PATY_I, ''))) = 0, 'RPR6206', '') AS svErrorCode,
		{{ ref('JoinAll') }}.CSE_FA_CLNT_UNTK_ID AS FA_CLIENT_UNDERTAKING_ID,
		{{ ref('JoinAll') }}.CSE_FA_UNTK_ID AS FA_UNDERTAKING_ID,
		{{ ref('JoinAll') }}.CSE_COIN_ENTY_ID AS COIN_ENTITY_ID,
		{{ ref('JoinAll') }}.CSE_CLNT_CORR_ID AS CLIENT_CORRELATION_ID,
		{{ ref('JoinAll') }}.CSE_FA_ENTY_CAT_ID AS FA_ENTITY_CAT_ID,
		{{ ref('JoinAll') }}.CSE_FA_CHLD_STAT_CAT_ID AS FA_CHILD_STATUS_CAT_ID,
		{{ ref('JoinAll') }}.CSE_CLNT_REL_TYPE_ID AS CLIENT_RELATIONSHIP_TYPE_ID,
		{{ ref('JoinAll') }}.CSE_CLNT_POSN AS CLIENT_POSITION,
		{{ ref('JoinAll') }}.CSE_IS_PRIM_FLAG AS IS_PRIMARY_FLAG,
		{{ ref('JoinAll') }}.CSE_CIF_CODE AS CIF_CODE,
		ETL_PROCESS_DT AS ETL_D,
		{{ ref('JoinAll') }}.CSE_ORIG_ETL_D AS ORIG_ETL_D,
		svErrorCode AS EROR_C
	FROM {{ ref('JoinAll') }}
	WHERE svErrorCode <> ''
)

SELECT * FROM XfmCheckDeltaAction__OutFAclientUndertakingRejectsDS