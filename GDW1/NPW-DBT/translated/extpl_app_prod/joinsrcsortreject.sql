{{ config(materialized='view', tags=['ExtPL_APP_PROD']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckPLAppProdIdNullsSorted') }}.PL_APP_PROD_ID,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckPLAppProdIdNullsSorted') }}.PL_TARGET_CAT_ID,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckPLAppProdIdNullsSorted') }}.REPAY_APPROX_AMT,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckPLAppProdIdNullsSorted') }}.REPAY_FREQUENCY_ID,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckPLAppProdIdNullsSorted') }}.PL_APP_PROD_REPAY_CAT_ID,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckPLAppProdIdNullsSorted') }}.PL_PROD_TERM_CAT_ID,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckPLAppProdIdNullsSorted') }}.PL_CAMPAIGN_CAT_ID,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckPLAppProdIdNullsSorted') }}.AD_HOC_CAMPAIGN_DESC,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckPLAppProdIdNullsSorted') }}.CAR_SEEKER_FLAG,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckPLAppProdIdNullsSorted') }}.PL_PROD_TARGET_CAT_ID,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckPLAppProdIdNullsSorted') }}.PL_MARKETING_SOURCE_CAT_ID,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckPLAppProdIdNullsSorted') }}.HLS_ACCT_NO,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckPLAppProdIdNullsSorted') }}.TOTAL_INTEREST_AMT,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckPLAppProdIdNullsSorted') }}.APP_PROD_AMT,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckPLAppProdIdNullsSorted') }}.TP_BROKER_ID,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckPLAppProdIdNullsSorted') }}.TP_BROKER_FIRST_NAME,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckPLAppProdIdNullsSorted') }}.TP_BROKER_LAST_NAME,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckPLAppProdIdNullsSorted') }}.TP_BROKER_GROUP_CAT_ID,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckPLAppProdIdNullsSorted') }}.READ_COSTS_AND_RISKS_FLAG,
		{{ ref('XfmCheckCCAppProdIdNulls__OutCheckPLAppProdIdNullsSorted') }}.ACCEPTS_COSTS_AND_RISKS_DATE,
		{{ ref('CpyRejtPLAppProdRejectOra') }}.PL_APP_PROD_ID AS PL_APP_PROD_ID_R,
		{{ ref('CpyRejtPLAppProdRejectOra') }}.PL_TARGET_CAT_ID_R,
		{{ ref('CpyRejtPLAppProdRejectOra') }}.REPAY_APPROX_AMT_R,
		{{ ref('CpyRejtPLAppProdRejectOra') }}.REPAY_FREQUENCY_ID_R,
		{{ ref('CpyRejtPLAppProdRejectOra') }}.PL_APP_PROD_REPAY_CAT_ID_R,
		{{ ref('CpyRejtPLAppProdRejectOra') }}.PL_PROD_TERM_CAT_ID_R,
		{{ ref('CpyRejtPLAppProdRejectOra') }}.PL_CAMPAIGN_CAT_ID_R,
		{{ ref('CpyRejtPLAppProdRejectOra') }}.AD_HOC_CAMPAIGN_DESC_R,
		{{ ref('CpyRejtPLAppProdRejectOra') }}.CAR_SEEKER_FLAG_R,
		{{ ref('CpyRejtPLAppProdRejectOra') }}.PL_PROD_TARGET_CAT_ID_R,
		{{ ref('CpyRejtPLAppProdRejectOra') }}.PL_MARKETING_SOURCE_CAT_ID_R,
		{{ ref('CpyRejtPLAppProdRejectOra') }}.HLS_ACCT_NO_R,
		{{ ref('CpyRejtPLAppProdRejectOra') }}.TOTAL_INTEREST_AMT_R,
		{{ ref('CpyRejtPLAppProdRejectOra') }}.APP_PROD_AMT_R,
		{{ ref('CpyRejtPLAppProdRejectOra') }}.TP_BROKER_ID_R,
		{{ ref('CpyRejtPLAppProdRejectOra') }}.TP_BROKER_FIRST_NAME_R,
		{{ ref('CpyRejtPLAppProdRejectOra') }}.TP_BROKER_LAST_NAME_R,
		{{ ref('CpyRejtPLAppProdRejectOra') }}.TP_BROKER_GROUP_CAT_ID_R,
		{{ ref('CpyRejtPLAppProdRejectOra') }}.READ_COSTS_AND_RISKS_FLAG_R,
		{{ ref('CpyRejtPLAppProdRejectOra') }}.ACCEPTS_COSTS_AND_RISKS_DATE_R,
		{{ ref('CpyRejtPLAppProdRejectOra') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmCheckCCAppProdIdNulls__OutCheckPLAppProdIdNullsSorted') }}
	OUTER JOIN {{ ref('CpyRejtPLAppProdRejectOra') }} ON {{ ref('XfmCheckCCAppProdIdNulls__OutCheckPLAppProdIdNullsSorted') }}.PL_APP_PROD_ID = {{ ref('CpyRejtPLAppProdRejectOra') }}.PL_APP_PROD_ID
)

SELECT * FROM JoinSrcSortReject