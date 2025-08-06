{{ config(materialized='view', tags=['ExtApptQstnAns']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec AS (
	SELECT
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((XfmSeparateRejects.APP_ID)) THEN (XfmSeparateRejects.APP_ID) ELSE ""))) = 0 Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.APP_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.APP_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		APP_ID,
		SUBTYPE_CODE,
		QA_QUESTION_ID,
		QA_ANSWER_ID,
		TEXT_ANSWER,
		CIF_CODE,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'S'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec