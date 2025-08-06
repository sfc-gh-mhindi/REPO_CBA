{{ config(materialized='view', tags=['ExtCplApptRel']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec AS (
	SELECT
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((XfmSeparateRejects.APPT_I)) THEN (XfmSeparateRejects.APPT_I) ELSE ""))) = 0 Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.APPT_I IS NOT NULL, {{ ref('JoinSrcSortReject') }}.APPT_I, ''))) = 0, 'R', 'S') AS DeltaFlag,
		LOAN_SUBTYPE_CODE,
		APPT_I,
		REL_TYPE_C,
		RELD_APPT_I,
		ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'S'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec