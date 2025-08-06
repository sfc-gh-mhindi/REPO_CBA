{{ config(materialized='view', tags=['ExtCCL_HL_APP']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.CCL_HL_APP_ID)) THEN (XfmSeparateRejects.CCL_HL_APP_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.CCL_HL_APP_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.CCL_HL_APP_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.CCL_HL_APP_ID_R AS CCL_HL_APP_ID,
		{{ ref('JoinSrcSortReject') }}.CCL_APP_ID_R AS CCL_APP_ID,
		{{ ref('JoinSrcSortReject') }}.HL_APP_ID_R AS HL_APP_ID,
		{{ ref('JoinSrcSortReject') }}.LMI_AMT_R AS LMI_AMT,
		{{ ref('JoinSrcSortReject') }}.HL_PACKAGE_CAT_ID_R AS HL_PACKAGE_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec