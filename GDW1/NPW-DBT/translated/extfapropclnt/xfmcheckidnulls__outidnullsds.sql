{{ config(materialized='view', tags=['ExtFaPropClnt']) }}

WITH XfmCheckIdNulls__OutIdNullsDS AS (
	SELECT
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((OutSrcFAProposedClient.FA_UNDERTAKING_ID)) THEN (OutSrcFAProposedClient.FA_UNDERTAKING_ID) ELSE ""))) = 0 or Len(Trim(( IF IsNotNull((OutSrcFAProposedClient.FA_PROPOSED_CLIENT_ID)) THEN (OutSrcFAProposedClient.FA_PROPOSED_CLIENT_ID) ELSE ""))) = 0 Then 'REJ6020' else '',
		IFF(LEN(TRIM(IFF({{ ref('TgtFaPropClntExtDelta') }}.FA_UNDERTAKING_ID IS NOT NULL, {{ ref('TgtFaPropClntExtDelta') }}.FA_UNDERTAKING_ID, ''))) = 0 OR LEN(TRIM(IFF({{ ref('TgtFaPropClntExtDelta') }}.FA_PROPOSED_CLIENT_ID IS NOT NULL, {{ ref('TgtFaPropClntExtDelta') }}.FA_PROPOSED_CLIENT_ID, ''))) = 0, 'REJ6020', '') AS ErrorCode,
		FA_PROPOSED_CLIENT_ID,
		COIN_ENTITY_ID,
		CLIENT_CORRELATION_ID,
		COIN_ENTITY_NAME,
		FA_ENTITY_CAT_ID,
		FA_UNDERTAKING_ID,
		FA_PROPOSED_CLIENT_CAT_ID,
		ETL_PROCESS_DT AS ETL_D,
		ETL_PROCESS_DT AS ORIG_ETL_D,
		ErrorCode AS EROR_C
	FROM {{ ref('TgtFaPropClntExtDelta') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) = 'REJ'
)

SELECT * FROM XfmCheckIdNulls__OutIdNullsDS