{{ config(materialized='view', tags=['ExtCSE_CCL_BUS_APP_FEE']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.CCL_APP_FEE_ID)) THEN (XfmSeparateRejects.CCL_APP_FEE_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.CCL_APP_FEE_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.CCL_APP_FEE_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		CCL_APP_FEE_ID,
		CCL_APP_FEE_CCL_APP_ID,
		CCL_APP_FEE_CCL_APP_PROD_ID,
		CCL_APP_FEE_CHARGE_AMT,
		-- *SRC*: \(20)if isNull(XfmSeparateRejects.CCL_APP_FEE_CHARGE_DATE) then ' ' else XfmSeparateRejects.CCL_APP_FEE_CHARGE_DATE,
		IFF({{ ref('JoinSrcSortReject') }}.CCL_APP_FEE_CHARGE_DATE IS NULL, ' ', {{ ref('JoinSrcSortReject') }}.CCL_APP_FEE_CHARGE_DATE) AS CCL_APP_FEE_CHARGE_DATE,
		CCL_APP_FEE_CONCESSION_FLAG,
		CCL_APP_FEE_CONCESSION_REASON,
		CCL_APP_FEE_OVERRIDE_FEE_PCT,
		CCL_APP_FEE_OVERRIDE_FEE_PCT_FREQ,
		CCL_APP_FEE_FEE_REPAYMENT_FREQ,
		CCL_APP_FEE_TYPE_CAT_ID,
		CCL_APP_FEE_CHARGE_EXTERNAL_FLAG,
		CCL_FEE_TYPE_CAT_ID,
		CCL_FEE_TYPE_CAT_DEFAULT_AMT,
		CCL_FEE_TYPE_CAT_DEFAULT_FEE_PCT,
		CCL_FEE_TYPE_CAT_FEE_TYPE_DESC,
		ETL_PROCESS_DT AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'S'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec