{{ config(materialized='view', tags=['ExtCclBusApp']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.CCL_APP_ID)) THEN (XfmSeparateRejects.CCL_APP_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.CCL_APP_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.CCL_APP_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		CCL_APP_ID,
		CCL_APP_CAT_ID,
		CCL_FORM_CAT_ID,
		TOTAL_PERSONAL_FAC_AMT,
		TOTAL_EQUIPMENTFINANCE_FAC_AMT,
		TOTAL_COMMERCIAL_FAC_AMT,
		TOPUP_APP_ID,
		AF_PRIMARY_INDUSTRY_ID,
		AD_TUC_AMT,
		COMMISSION_AMT,
		BROKER_REFERAL_FLAG,
		CARNELL_EXPOSURE_AMT,
		CARNELL_EXPOSURE_AMT_DATE,
		CARNELL_OVERRIDE_COV_ASSESSMNT,
		CARNELL_OVERRIDE_REASON_CAT_ID,
		CARNELL_SHORT_DEFAULT_OVERRIDE,
		SRCE_REC,
		ETL_PROCESS_DT AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'S'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec