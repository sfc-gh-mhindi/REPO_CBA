{{ config(materialized='view', tags=['ExtCclBusApp']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckCclAppIdNulls__OutCheckCclAppIdNullsSorted') }}.CCL_APP_ID,
		{{ ref('XfmCheckCclAppIdNulls__OutCheckCclAppIdNullsSorted') }}.CCL_APP_CAT_ID,
		{{ ref('XfmCheckCclAppIdNulls__OutCheckCclAppIdNullsSorted') }}.CCL_FORM_CAT_ID,
		{{ ref('XfmCheckCclAppIdNulls__OutCheckCclAppIdNullsSorted') }}.TOTAL_PERSONAL_FAC_AMT,
		{{ ref('XfmCheckCclAppIdNulls__OutCheckCclAppIdNullsSorted') }}.TOTAL_EQUIPMENTFINANCE_FAC_AMT,
		{{ ref('XfmCheckCclAppIdNulls__OutCheckCclAppIdNullsSorted') }}.TOTAL_COMMERCIAL_FAC_AMT,
		{{ ref('XfmCheckCclAppIdNulls__OutCheckCclAppIdNullsSorted') }}.TOPUP_APP_ID,
		{{ ref('XfmCheckCclAppIdNulls__OutCheckCclAppIdNullsSorted') }}.AF_PRIMARY_INDUSTRY_ID,
		{{ ref('XfmCheckCclAppIdNulls__OutCheckCclAppIdNullsSorted') }}.AD_TUC_AMT,
		{{ ref('XfmCheckCclAppIdNulls__OutCheckCclAppIdNullsSorted') }}.COMMISSION_AMT,
		{{ ref('XfmCheckCclAppIdNulls__OutCheckCclAppIdNullsSorted') }}.BROKER_REFERAL_FLAG,
		{{ ref('XfmCheckCclAppIdNulls__OutCheckCclAppIdNullsSorted') }}.CARNELL_EXPOSURE_AMT,
		XfmCheckCclAppIdNulls__XfmCheckCclAppIdNulls__OutCheckCclAppIdNullsSorted.CARNELL_EXPOSURE_AMT_DATE,
		{{ ref('XfmCheckCclAppIdNulls__OutCheckCclAppIdNullsSorted') }}.CARNELL_OVERRIDE_COV_ASSESSMNT,
		{{ ref('XfmCheckCclAppIdNulls__OutCheckCclAppIdNullsSorted') }}.CARNELL_OVERRIDE_REASON_CAT_ID,
		{{ ref('XfmCheckCclAppIdNulls__OutCheckCclAppIdNullsSorted') }}.CARNELL_SHORT_DEFAULT_OVERRIDE,
		{{ ref('XfmCheckCclAppIdNulls__OutCheckCclAppIdNullsSorted') }}.SRCE_REC,
		{{ ref('CpyRejtCclAppRejectOra') }}.CCL_APP_ID AS CCL_APP_ID_R,
		{{ ref('CpyRejtCclAppRejectOra') }}.CCL_APP_CAT_ID_R,
		{{ ref('CpyRejtCclAppRejectOra') }}.CCL_FORM_CAT_ID_R,
		{{ ref('CpyRejtCclAppRejectOra') }}.TOTAL_PERSONAL_FAC_AMT_R,
		{{ ref('CpyRejtCclAppRejectOra') }}.TOTAL_EQUIPMENTFINANCE_FAC_AMT_R,
		{{ ref('CpyRejtCclAppRejectOra') }}.TOTAL_COMMERCIAL_FAC_AMT_R,
		{{ ref('CpyRejtCclAppRejectOra') }}.TOPUP_APP_ID_R,
		{{ ref('CpyRejtCclAppRejectOra') }}.AF_PRIMARY_INDUSTRY_ID_R,
		{{ ref('CpyRejtCclAppRejectOra') }}.AD_TUC_AMT_R,
		{{ ref('CpyRejtCclAppRejectOra') }}.COMMISSION_AMT_R,
		{{ ref('CpyRejtCclAppRejectOra') }}.BROKER_REFERAL_FLAG_R,
		{{ ref('CpyRejtCclAppRejectOra') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmCheckCclAppIdNulls__OutCheckCclAppIdNullsSorted') }}
	OUTER JOIN {{ ref('CpyRejtCclAppRejectOra') }} ON {{ ref('XfmCheckCclAppIdNulls__OutCheckCclAppIdNullsSorted') }}.CCL_APP_ID = {{ ref('CpyRejtCclAppRejectOra') }}.CCL_APP_ID
)

SELECT * FROM JoinSrcSortReject