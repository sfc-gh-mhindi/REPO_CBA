{{ config(materialized='view', tags=['ExtFaUndertaking']) }}

WITH XfmCheckIdNulls__OutCheckIdNullsSorted AS (
	SELECT
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((OutSrcFAUndertaking.FA_UNDERTAKING_ID)) THEN (OutSrcFAUndertaking.FA_UNDERTAKING_ID) ELSE ""))) = 0 Then 'REJ6001' Else '',
		IFF(LEN(TRIM(IFF({{ ref('SrcFAUndertaking') }}.FA_UNDERTAKING_ID IS NOT NULL, {{ ref('SrcFAUndertaking') }}.FA_UNDERTAKING_ID, ''))) = 0, 'REJ6001', '') AS ErrorCode,
		FA_UNDERTAKING_ID,
		PLANNING_GROUP_NAME,
		COIN_ADVICE_GROUP_ID,
		ADVICE_GROUP_CORRELATION_ID,
		CREATED_DATE,
		CREATED_BY_STAFF_NUMBER,
		SM_CASE_ID
	FROM {{ ref('SrcFAUndertaking') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) <> 'REJ'
)

SELECT * FROM XfmCheckIdNulls__OutCheckIdNullsSorted