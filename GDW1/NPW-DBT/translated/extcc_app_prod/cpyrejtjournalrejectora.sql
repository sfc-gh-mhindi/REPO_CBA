{{ config(materialized='view', tags=['ExtCC_APP_PROD']) }}

WITH CpyRejtJournalRejectOra AS (
	SELECT
		CC_APP_PROD_ID,
		{{ ref('SrcRejtCCAppProdRejectOra') }}.REQUESTED_LIMIT_AMT AS REQUESTED_LIMIT_AMT_R,
		{{ ref('SrcRejtCCAppProdRejectOra') }}.CC_INTEREST_OPT_CAT_ID AS CC_INTEREST_OPT_CAT_ID_R,
		{{ ref('SrcRejtCCAppProdRejectOra') }}.CBA_HOMELOAN_NO AS CBA_HOMELOAN_NO_R,
		{{ ref('SrcRejtCCAppProdRejectOra') }}.PRE_APPRV_AMOUNT AS PRE_APPRV_AMOUNT_R,
		{{ ref('SrcRejtCCAppProdRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R,
		{{ ref('SrcRejtCCAppProdRejectOra') }}.READ_COSTS_AND_RISKS_FLAG AS READ_COSTS_AND_RISKS_FLAG_R,
		{{ ref('SrcRejtCCAppProdRejectOra') }}.ACCEPTS_COSTS_AND_RISKS_DATE AS ACCEPTS_COSTS_AND_RISKS_DATE_R
	FROM {{ ref('SrcRejtCCAppProdRejectOra') }}
)

SELECT * FROM CpyRejtJournalRejectOra