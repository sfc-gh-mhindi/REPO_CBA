{{ config(materialized='view', tags=['ExtCclBusApp']) }}

WITH CpyRejtCclAppRejectOra AS (
	SELECT
		CCL_APP_ID,
		{{ ref('SrcRejtCclAppRejectOra') }}.CCL_APP_CAT_ID AS CCL_APP_CAT_ID_R,
		{{ ref('SrcRejtCclAppRejectOra') }}.CCL_FORM_CAT_ID AS CCL_FORM_CAT_ID_R,
		{{ ref('SrcRejtCclAppRejectOra') }}.TOTAL_PERSONAL_FAC_AMT AS TOTAL_PERSONAL_FAC_AMT_R,
		{{ ref('SrcRejtCclAppRejectOra') }}.TOTAL_EQUIPMENTFINANCE_FAC_AMT AS TOTAL_EQUIPMENTFINANCE_FAC_AMT_R,
		{{ ref('SrcRejtCclAppRejectOra') }}.TOTAL_COMMERCIAL_FAC_AMT AS TOTAL_COMMERCIAL_FAC_AMT_R,
		{{ ref('SrcRejtCclAppRejectOra') }}.TOPUP_APP_ID AS TOPUP_APP_ID_R,
		{{ ref('SrcRejtCclAppRejectOra') }}.AF_PRIMARY_INDUSTRY_ID AS AF_PRIMARY_INDUSTRY_ID_R,
		{{ ref('SrcRejtCclAppRejectOra') }}.AD_TUC_AMT AS AD_TUC_AMT_R,
		{{ ref('SrcRejtCclAppRejectOra') }}.COMMISSION_AMT AS COMMISSION_AMT_R,
		{{ ref('SrcRejtCclAppRejectOra') }}.BROKER_REFERAL_FLAG AS BROKER_REFERAL_FLAG_R,
		{{ ref('SrcRejtCclAppRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtCclAppRejectOra') }}
)

SELECT * FROM CpyRejtCclAppRejectOra