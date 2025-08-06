{{ config(materialized='view', tags=['ExtFaEnvisionEvent']) }}

WITH XfmCheckIdNulls__OutCheckIdNullsSorted AS (
	SELECT
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((OutSrcFAUndertaking.FA_ENVISION_EVENT_ID)) THEN (OutSrcFAUndertaking.FA_ENVISION_EVENT_ID) ELSE ""))) = 0 Then 'REJ6002' Else '',
		IFF(LEN(TRIM(IFF({{ ref('SrcFAEnvisionEvent') }}.FA_ENVISION_EVENT_ID IS NOT NULL, {{ ref('SrcFAEnvisionEvent') }}.FA_ENVISION_EVENT_ID, ''))) = 0, 'REJ6002', '') AS ErrorCode,
		FA_ENVISION_EVENT_ID,
		FA_UNDERTAKING_ID,
		FA_ENVISION_EVENT_CAT_ID,
		CREATED_DATE,
		CREATED_BY_STAFF_NUMBER,
		COIN_REQUEST_ID
	FROM {{ ref('SrcFAEnvisionEvent') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) <> 'REJ'
)

SELECT * FROM XfmCheckIdNulls__OutCheckIdNullsSorted