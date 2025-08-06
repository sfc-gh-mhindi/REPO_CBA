{{ config(materialized='view', tags=['ExtCSE_CCL_BUS_APP_FEE']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckCCL_APP_FEE_IDNulls__OutCheckCCL_APP_FEE_IDNullsSorted') }}.CCL_APP_FEE_ID,
		{{ ref('XfmCheckCCL_APP_FEE_IDNulls__OutCheckCCL_APP_FEE_IDNullsSorted') }}.CCL_APP_FEE_CCL_APP_ID,
		{{ ref('XfmCheckCCL_APP_FEE_IDNulls__OutCheckCCL_APP_FEE_IDNullsSorted') }}.CCL_APP_FEE_CCL_APP_PROD_ID,
		{{ ref('XfmCheckCCL_APP_FEE_IDNulls__OutCheckCCL_APP_FEE_IDNullsSorted') }}.CCL_APP_FEE_CHARGE_AMT,
		{{ ref('XfmCheckCCL_APP_FEE_IDNulls__OutCheckCCL_APP_FEE_IDNullsSorted') }}.CCL_APP_FEE_CHARGE_DATE,
		{{ ref('XfmCheckCCL_APP_FEE_IDNulls__OutCheckCCL_APP_FEE_IDNullsSorted') }}.CCL_APP_FEE_CONCESSION_FLAG,
		{{ ref('XfmCheckCCL_APP_FEE_IDNulls__OutCheckCCL_APP_FEE_IDNullsSorted') }}.CCL_APP_FEE_CONCESSION_REASON,
		{{ ref('XfmCheckCCL_APP_FEE_IDNulls__OutCheckCCL_APP_FEE_IDNullsSorted') }}.CCL_APP_FEE_OVERRIDE_FEE_PCT,
		XfmCheckCCL_APP_FEE_IDNulls__XfmCheckCCL_APP_FEE_IDNulls__OutCheckCCL_APP_FEE_IDNullsSorted.CCL_APP_FEE_OVERRIDE_FEE_PCT_FREQ,
		{{ ref('XfmCheckCCL_APP_FEE_IDNulls__OutCheckCCL_APP_FEE_IDNullsSorted') }}.CCL_APP_FEE_FEE_REPAYMENT_FREQ,
		{{ ref('XfmCheckCCL_APP_FEE_IDNulls__OutCheckCCL_APP_FEE_IDNullsSorted') }}.CCL_APP_FEE_TYPE_CAT_ID,
		{{ ref('XfmCheckCCL_APP_FEE_IDNulls__OutCheckCCL_APP_FEE_IDNullsSorted') }}.CCL_APP_FEE_CHARGE_EXTERNAL_FLAG,
		{{ ref('XfmCheckCCL_APP_FEE_IDNulls__OutCheckCCL_APP_FEE_IDNullsSorted') }}.CCL_FEE_TYPE_CAT_ID,
		{{ ref('XfmCheckCCL_APP_FEE_IDNulls__OutCheckCCL_APP_FEE_IDNullsSorted') }}.CCL_FEE_TYPE_CAT_DEFAULT_AMT,
		{{ ref('XfmCheckCCL_APP_FEE_IDNulls__OutCheckCCL_APP_FEE_IDNullsSorted') }}.CCL_FEE_TYPE_CAT_DEFAULT_FEE_PCT,
		{{ ref('XfmCheckCCL_APP_FEE_IDNulls__OutCheckCCL_APP_FEE_IDNullsSorted') }}.CCL_FEE_TYPE_CAT_FEE_TYPE_DESC,
		{{ ref('CpyRejtJournalRejectOra') }}.CCL_APP_FEE_ID AS CCL_APP_FEE_ID_R,
		{{ ref('CpyRejtJournalRejectOra') }}.CCL_APP_FEE_CCL_APP_ID_R,
		{{ ref('CpyRejtJournalRejectOra') }}.CCL_APP_FEE_CCL_APP_PROD_ID_R,
		{{ ref('CpyRejtJournalRejectOra') }}.CCL_APP_FEE_CHARGE_AMT_R,
		{{ ref('CpyRejtJournalRejectOra') }}.CCL_APP_FEE_CHARGE_DATE_R,
		{{ ref('CpyRejtJournalRejectOra') }}.CCL_APP_FEE_CONCESSION_FLAG_R,
		{{ ref('CpyRejtJournalRejectOra') }}.CCL_APP_FEE_CONCESSION_REASON_R,
		{{ ref('CpyRejtJournalRejectOra') }}.CCL_APP_FEE_OVERRIDE_FEE_PCT_R,
		{{ ref('CpyRejtJournalRejectOra') }}.CCL_APP_FEE_OVERRIDE_FEE_PCT_FREQ_R,
		{{ ref('CpyRejtJournalRejectOra') }}.CCL_APP_FEE_FEE_REPAYMENT_FREQ_R,
		{{ ref('CpyRejtJournalRejectOra') }}.CCL_APP_FEE_TYPE_CAT_ID_R,
		{{ ref('CpyRejtJournalRejectOra') }}.CCL_APP_FEE_CHARGE_EXTERNAL_FLAG_R,
		{{ ref('CpyRejtJournalRejectOra') }}.CCL_FEE_TYPE_CAT_ID_R,
		{{ ref('CpyRejtJournalRejectOra') }}.CCL_FEE_TYPE_CAT_DEFAULT_AMT_R,
		{{ ref('CpyRejtJournalRejectOra') }}.CCL_FEE_TYPE_CAT_DEFAULT_FEE_PCT_R,
		{{ ref('CpyRejtJournalRejectOra') }}.CCL_FEE_TYPE_CAT_FEE_TYPE_DESC_R,
		{{ ref('CpyRejtJournalRejectOra') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmCheckCCL_APP_FEE_IDNulls__OutCheckCCL_APP_FEE_IDNullsSorted') }}
	OUTER JOIN {{ ref('CpyRejtJournalRejectOra') }} ON {{ ref('XfmCheckCCL_APP_FEE_IDNulls__OutCheckCCL_APP_FEE_IDNullsSorted') }}.CCL_APP_FEE_ID = {{ ref('CpyRejtJournalRejectOra') }}.CCL_APP_FEE_ID
)

SELECT * FROM JoinSrcSortReject