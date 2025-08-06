{{ config(materialized='view', tags=['Ext_APP_ANS']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.HL_APP_ID)) THEN (XfmSeparateRejects.HL_APP_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.HL_APP_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.HL_APP_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.HL_APP_ID_R AS HL_APP_ID,
		{{ ref('JoinSrcSortReject') }}.QA_QUESTION_ID_R AS QA_QUESTION_ID,
		{{ ref('JoinSrcSortReject') }}.QA_ANSWER_ID_R AS QA_ANSWER_ID,
		{{ ref('JoinSrcSortReject') }}.TEXT_ANSWER_R AS TEXT_ANSWER,
		{{ ref('JoinSrcSortReject') }}.CIF_CODE_R AS CIF_CODE,
		{{ ref('JoinSrcSortReject') }}.CBA_STAFF_NUMBER_R AS CBA_STAFF_NUMBER,
		{{ ref('JoinSrcSortReject') }}.LODGEMENT_BRANCH_ID_R AS LODGEMENT_BRANCH_ID,
		{{ ref('JoinSrcSortReject') }}.SUBTYPE_CODE_R AS SUBTYPE_CODE,
		{{ ref('JoinSrcSortReject') }}.MOD_TIMESTAMP_R AS MOD_TIMESTAMP,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec