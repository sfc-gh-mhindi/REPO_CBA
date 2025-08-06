{{ config(materialized='view', tags=['ExtCclBusApp']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.CCL_APP_ID)) THEN (XfmSeparateRejects.CCL_APP_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.CCL_APP_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.CCL_APP_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.CCL_APP_ID_R AS CCL_APP_ID,
		{{ ref('JoinSrcSortReject') }}.CCL_APP_CAT_ID_R AS CCL_APP_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.CCL_FORM_CAT_ID_R AS CCL_FORM_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.TOTAL_PERSONAL_FAC_AMT_R AS TOTAL_PERSONAL_FAC_AMT,
		{{ ref('JoinSrcSortReject') }}.TOTAL_EQUIPMENTFINANCE_FAC_AMT_R AS TOTAL_EQUIPMENTFINANCE_FAC_AMT,
		{{ ref('JoinSrcSortReject') }}.TOTAL_COMMERCIAL_FAC_AMT_R AS TOTAL_COMMERCIAL_FAC_AMT,
		{{ ref('JoinSrcSortReject') }}.TOPUP_APP_ID_R AS TOPUP_APP_ID,
		{{ ref('JoinSrcSortReject') }}.AF_PRIMARY_INDUSTRY_ID_R AS AF_PRIMARY_INDUSTRY_ID,
		{{ ref('JoinSrcSortReject') }}.AD_TUC_AMT_R AS AD_TUC_AMT,
		{{ ref('JoinSrcSortReject') }}.COMMISSION_AMT_R AS COMMISSION_AMT,
		{{ ref('JoinSrcSortReject') }}.BROKER_REFERAL_FLAG_R AS BROKER_REFERAL_FLAG,
		-- *SRC*: SetNull(),
		SETNULL() AS CARNELL_EXPOSURE_AMT,
		-- *SRC*: SetNull(),
		SETNULL() AS CARNELL_EXPOSURE_AMT_DATE,
		-- *SRC*: SetNull(),
		SETNULL() AS CARNELL_OVERRIDE_COV_ASSESSMNT,
		-- *SRC*: SetNull(),
		SETNULL() AS CARNELL_OVERRIDE_REASON_CAT_ID,
		-- *SRC*: SetNull(),
		SETNULL() AS CARNELL_SHORT_DEFAULT_OVERRIDE,
		'N' AS SRCE_REC,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec