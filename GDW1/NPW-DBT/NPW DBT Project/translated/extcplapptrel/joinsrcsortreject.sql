{{ config(materialized='view', tags=['ExtCplApptRel']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckCCAppProdBalXferIdNulls__OutCheckCCAppProdBalXferIdNullsSorted') }}.LOAN_SUBTYPE_CODE,
		{{ ref('XfmCheckCCAppProdBalXferIdNulls__OutCheckCCAppProdBalXferIdNullsSorted') }}.APPT_I,
		{{ ref('XfmCheckCCAppProdBalXferIdNulls__OutCheckCCAppProdBalXferIdNullsSorted') }}.REL_TYPE_C,
		{{ ref('XfmCheckCCAppProdBalXferIdNulls__OutCheckCCAppProdBalXferIdNullsSorted') }}.RELD_APPT_I,
		{{ ref('CpyRejtCCAppProdBalXferRejectOra') }}.APPT_I AS APPT_I_R,
		{{ ref('CpyRejtCCAppProdBalXferRejectOra') }}.RELD_APPT_I_R,
		{{ ref('CpyRejtCCAppProdBalXferRejectOra') }}.REL_TYPE_C_R,
		{{ ref('CpyRejtCCAppProdBalXferRejectOra') }}.LOAN_SUBTYPE_CODE_R,
		{{ ref('CpyRejtCCAppProdBalXferRejectOra') }}.ORIG_ETL_D
	FROM {{ ref('XfmCheckCCAppProdBalXferIdNulls__OutCheckCCAppProdBalXferIdNullsSorted') }}
	OUTER JOIN {{ ref('CpyRejtCCAppProdBalXferRejectOra') }} ON {{ ref('XfmCheckCCAppProdBalXferIdNulls__OutCheckCCAppProdBalXferIdNullsSorted') }}.APPT_I = {{ ref('CpyRejtCCAppProdBalXferRejectOra') }}.APPT_I
)

SELECT * FROM JoinSrcSortReject