{{ config(materialized='view', tags=['ExtApptQstnAns']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckCCAppProdBalXferIdNulls__OutCheckCCAppProdBalXferIdNullsSorted') }}.APP_ID,
		{{ ref('XfmCheckCCAppProdBalXferIdNulls__OutCheckCCAppProdBalXferIdNullsSorted') }}.SUBTYPE_CODE,
		{{ ref('XfmCheckCCAppProdBalXferIdNulls__OutCheckCCAppProdBalXferIdNullsSorted') }}.QA_QUESTION_ID,
		{{ ref('XfmCheckCCAppProdBalXferIdNulls__OutCheckCCAppProdBalXferIdNullsSorted') }}.QA_ANSWER_ID,
		{{ ref('XfmCheckCCAppProdBalXferIdNulls__OutCheckCCAppProdBalXferIdNullsSorted') }}.TEXT_ANSWER,
		{{ ref('XfmCheckCCAppProdBalXferIdNulls__OutCheckCCAppProdBalXferIdNullsSorted') }}.CIF_CODE,
		{{ ref('CpyRejtCCAppProdBalXferRejectOra') }}.APP_ID AS APP_ID_R,
		{{ ref('CpyRejtCCAppProdBalXferRejectOra') }}.SUBTYPE_CODE_R,
		{{ ref('CpyRejtCCAppProdBalXferRejectOra') }}.QA_QUESTION_ID_R,
		{{ ref('CpyRejtCCAppProdBalXferRejectOra') }}.QA_ANSWER_ID_R,
		{{ ref('CpyRejtCCAppProdBalXferRejectOra') }}.TEXT_ANSWER_R,
		{{ ref('CpyRejtCCAppProdBalXferRejectOra') }}.CIF_CODE_R,
		{{ ref('CpyRejtCCAppProdBalXferRejectOra') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmCheckCCAppProdBalXferIdNulls__OutCheckCCAppProdBalXferIdNullsSorted') }}
	OUTER JOIN {{ ref('CpyRejtCCAppProdBalXferRejectOra') }} ON {{ ref('XfmCheckCCAppProdBalXferIdNulls__OutCheckCCAppProdBalXferIdNullsSorted') }}.APP_ID = {{ ref('CpyRejtCCAppProdBalXferRejectOra') }}.APP_ID
)

SELECT * FROM JoinSrcSortReject