{{ config(materialized='view', tags=['ExtCplApptRel']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((XfmSeparateRejects.APPT_I)) THEN (XfmSeparateRejects.APPT_I) ELSE ""))) = 0 Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.APPT_I IS NOT NULL, {{ ref('JoinSrcSortReject') }}.APPT_I, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.LOAN_SUBTYPE_CODE_R AS LOAN_SUBTYPE_CODE,
		{{ ref('JoinSrcSortReject') }}.APPT_I_R AS APPT_I,
		{{ ref('JoinSrcSortReject') }}.REL_TYPE_C_R AS REL_TYPE_C,
		{{ ref('JoinSrcSortReject') }}.RELD_APPT_I_R AS RELD_APPT_I,
		ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec