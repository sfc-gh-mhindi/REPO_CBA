{{ config(materialized='view', tags=['ExtBUS_HLM_APP']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmNotNullCheck') }}.APP_ID,
		{{ ref('XfmNotNullCheck') }}.HLM_ACCOUNT_ID,
		{{ ref('XfmNotNullCheck') }}.ACCOUNT_NUMBER,
		{{ ref('XfmNotNullCheck') }}.CRIS_PRODUCT_ID,
		{{ ref('XfmNotNullCheck') }}.HLM_APP_TYPE_CAT_ID,
		{{ ref('XfmNotNullCheck') }}.DCHG_REAS_ID,
		{{ ref('XfmNotNullCheck') }}.HL_APP_PROD_ID,
		{{ ref('CpyRejtHlmAppRejectOra') }}.APP_ID AS rightRec_APP_ID,
		{{ ref('CpyRejtHlmAppRejectOra') }}.HLM_ACCOUNT_ID_R,
		{{ ref('CpyRejtHlmAppRejectOra') }}.ACCOUNT_NUMBER_R,
		{{ ref('CpyRejtHlmAppRejectOra') }}.CRIS_PRODUCT_ID_R,
		{{ ref('CpyRejtHlmAppRejectOra') }}.HLM_APP_TYPE_CAT_ID_R,
		{{ ref('CpyRejtHlmAppRejectOra') }}.DISCHARGE_REASON_ID_R,
		{{ ref('CpyRejtHlmAppRejectOra') }}.HL_APP_PROD_ID_R,
		{{ ref('CpyRejtHlmAppRejectOra') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmNotNullCheck') }}
	OUTER JOIN {{ ref('CpyRejtHlmAppRejectOra') }} ON {{ ref('XfmNotNullCheck') }}.APP_ID = {{ ref('CpyRejtHlmAppRejectOra') }}.APP_ID
)

SELECT * FROM JoinSrcSortReject