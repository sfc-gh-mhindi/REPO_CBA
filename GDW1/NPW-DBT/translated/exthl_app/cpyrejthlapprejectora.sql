{{ config(materialized='view', tags=['ExtHL_APP']) }}

WITH CpyRejtHlAppRejectOra AS (
	SELECT
		HL_APP_ID,
		HL_APP_PROD_ID,
		{{ ref('SrcRejtHlAppRejectOra') }}.HL_PACKAGE_CAT_ID AS HL_PACKAGE_CAT_ID_R,
		{{ ref('SrcRejtHlAppRejectOra') }}.LPC_OFFICE AS LPC_OFFICE_R,
		{{ ref('SrcRejtHlAppRejectOra') }}.STATUS_TRACKER_ID AS STATUS_TRACKER_ID_R,
		{{ ref('SrcRejtHlAppRejectOra') }}.CHL_APP_PCD_EXT_SYS_CAT_ID AS CHL_APP_PCD_EXT_SYS_CAT_ID_R,
		{{ ref('SrcRejtHlAppRejectOra') }}.CHL_APP_SIMPLE_APP_FLAG AS CHL_APP_SIMPLE_APP_FLAG_R,
		{{ ref('SrcRejtHlAppRejectOra') }}.CHL_APP_ORIGINATING_AGENT_ID AS CHL_APP_ORIGINATING_AGENT_ID_R,
		{{ ref('SrcRejtHlAppRejectOra') }}.CHL_APP_AGENT_NAME AS CHL_APP_AGENT_NAME_R,
		{{ ref('SrcRejtHlAppRejectOra') }}.CASS_WITHHOLD_RISKBANK_FLAG AS CASS_WITHHOLD_RISKBANK_FLAG_R,
		{{ ref('SrcRejtHlAppRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R,
		{{ ref('SrcRejtHlAppRejectOra') }}.CR_DATE AS CR_DATE_R,
		{{ ref('SrcRejtHlAppRejectOra') }}.ASSESSMENT_DATE AS ASSESSMENT_DATE_R,
		{{ ref('SrcRejtHlAppRejectOra') }}.NCPR_FLAG AS NCPR_FLAG_R,
		{{ ref('SrcRejtHlAppRejectOra') }}.FHB_FLAG AS FHB_FLAG_R,
		{{ ref('SrcRejtHlAppRejectOra') }}.SETTLEMENT_DATE AS SETTLEMENT_DATE_R,
		{{ ref('SrcRejtHlAppRejectOra') }}.PEXA_FLAG AS PEXA_FLAG_R,
		{{ ref('SrcRejtHlAppRejectOra') }}.HSCA_FLAG AS HSCA_FLAG_R,
		{{ ref('SrcRejtHlAppRejectOra') }}.HSCA_CONVERTED_TO_FULL_AT AS HSCA_CONVERTED_TO_FULL_AT_R
	FROM {{ ref('SrcRejtHlAppRejectOra') }}
)

SELECT * FROM CpyRejtHlAppRejectOra