{{ config(materialized='view', tags=['ExtCSE_CCL_BUS_APP_FEE']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.CCL_APP_FEE_ID)) THEN (XfmSeparateRejects.CCL_APP_FEE_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.CCL_APP_FEE_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.CCL_APP_FEE_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.CCL_APP_FEE_ID_R AS CCL_APP_FEE_ID,
		{{ ref('JoinSrcSortReject') }}.CCL_APP_FEE_CCL_APP_ID_R AS CCL_APP_FEE_CCL_APP_ID,
		{{ ref('JoinSrcSortReject') }}.CCL_APP_FEE_CCL_APP_PROD_ID_R AS CCL_APP_FEE_CCL_APP_PROD_ID,
		{{ ref('JoinSrcSortReject') }}.CCL_APP_FEE_CHARGE_AMT_R AS CCL_APP_FEE_CHARGE_AMT,
		-- *SRC*: \(20)if isNull(XfmSeparateRejects.CCL_APP_FEE_CHARGE_DATE_R) then ' ' else XfmSeparateRejects.CCL_APP_FEE_CHARGE_DATE_R,
		IFF({{ ref('JoinSrcSortReject') }}.CCL_APP_FEE_CHARGE_DATE_R IS NULL, ' ', {{ ref('JoinSrcSortReject') }}.CCL_APP_FEE_CHARGE_DATE_R) AS CCL_APP_FEE_CHARGE_DATE,
		{{ ref('JoinSrcSortReject') }}.CCL_APP_FEE_CONCESSION_FLAG_R AS CCL_APP_FEE_CONCESSION_FLAG,
		{{ ref('JoinSrcSortReject') }}.CCL_APP_FEE_CONCESSION_REASON_R AS CCL_APP_FEE_CONCESSION_REASON,
		{{ ref('JoinSrcSortReject') }}.CCL_APP_FEE_OVERRIDE_FEE_PCT_R AS CCL_APP_FEE_OVERRIDE_FEE_PCT,
		{{ ref('JoinSrcSortReject') }}.CCL_APP_FEE_OVERRIDE_FEE_PCT_FREQ_R AS CCL_APP_FEE_OVERRIDE_FEE_PCT_FREQ,
		{{ ref('JoinSrcSortReject') }}.CCL_APP_FEE_FEE_REPAYMENT_FREQ_R AS CCL_APP_FEE_FEE_REPAYMENT_FREQ,
		{{ ref('JoinSrcSortReject') }}.CCL_APP_FEE_TYPE_CAT_ID_R AS CCL_APP_FEE_TYPE_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.CCL_APP_FEE_CHARGE_EXTERNAL_FLAG_R AS CCL_APP_FEE_CHARGE_EXTERNAL_FLAG,
		{{ ref('JoinSrcSortReject') }}.CCL_FEE_TYPE_CAT_ID_R AS CCL_FEE_TYPE_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.CCL_FEE_TYPE_CAT_DEFAULT_AMT_R AS CCL_FEE_TYPE_CAT_DEFAULT_AMT,
		{{ ref('JoinSrcSortReject') }}.CCL_FEE_TYPE_CAT_DEFAULT_FEE_PCT_R AS CCL_FEE_TYPE_CAT_DEFAULT_FEE_PCT,
		{{ ref('JoinSrcSortReject') }}.CCL_FEE_TYPE_CAT_FEE_TYPE_DESC_R AS CCL_FEE_TYPE_CAT_FEE_TYPE_DESC,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec