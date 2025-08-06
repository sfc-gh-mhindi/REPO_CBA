{{ config(materialized='view', tags=['ExtCC_APP_PROD']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckCCAppProdIdNullsSorted') }}.CC_APP_PROD_ID,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckCCAppProdIdNullsSorted') }}.REQUESTED_LIMIT_AMT,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckCCAppProdIdNullsSorted') }}.CC_INTEREST_OPT_CAT_ID,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckCCAppProdIdNullsSorted') }}.CBA_HOMELOAN_NO,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckCCAppProdIdNullsSorted') }}.PRE_APPRV_AMOUNT,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckCCAppProdIdNullsSorted') }}.NTM_CAMPAIGN_ID,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckCCAppProdIdNullsSorted') }}.FRIES_CAMPAIGN_CODE,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckCCAppProdIdNullsSorted') }}.OAP_CAMPAIGN_CODE,
		{{ ref('CpyRejtJournalRejectOra') }}.CC_APP_PROD_ID AS CC_APP_PROD_ID_R,
		{{ ref('CpyRejtJournalRejectOra') }}.REQUESTED_LIMIT_AMT_R,
		{{ ref('CpyRejtJournalRejectOra') }}.CC_INTEREST_OPT_CAT_ID_R,
		{{ ref('CpyRejtJournalRejectOra') }}.CBA_HOMELOAN_NO_R,
		{{ ref('CpyRejtJournalRejectOra') }}.PRE_APPRV_AMOUNT_R,
		{{ ref('CpyRejtJournalRejectOra') }}.ORIG_ETL_D_R,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckCCAppProdIdNullsSorted') }}.READ_COSTS_AND_RISKS_FLAG,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckCCAppProdIdNullsSorted') }}.ACCEPTS_COSTS_AND_RISKS_DATE,
		{{ ref('CpyRejtJournalRejectOra') }}.READ_COSTS_AND_RISKS_FLAG_R,
		{{ ref('CpyRejtJournalRejectOra') }}.ACCEPTS_COSTS_AND_RISKS_DATE_R
	FROM {{ ref('XfmCheckCCAppProdIdNulls__OutCheckCCAppProdIdNullsSorted') }}
	OUTER JOIN {{ ref('CpyRejtJournalRejectOra') }} ON {{ ref('XfmCheckCCAppProdIdNulls__OutCheckCCAppProdIdNullsSorted') }}.CC_APP_PROD_ID = {{ ref('CpyRejtJournalRejectOra') }}.CC_APP_PROD_ID
)

SELECT * FROM JoinSrcSortReject