{{ config(materialized='view', tags=['ExtHL_APP']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}.HL_APP_ID,
		{{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}.HL_APP_PROD_ID,
		{{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}.HL_PACKAGE_CAT_ID,
		{{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}.LPC_OFFICE,
		{{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}.STATUS_TRACKER_ID,
		{{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}.CHL_APP_PCD_EXT_SYS_CAT_ID AS CHL_APP_PCD_EXTERNAL_SYS_CAT_ID,
		{{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}.CHL_APP_SIMPLE_APP_FLAG,
		{{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}.CHL_APP_ORIGINATING_AGENT_ID,
		{{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}.CHL_APP_AGENT_NAME,
		{{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}.CASS_WITHHOLD_RISKBANK_FLAG,
		{{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}.CR_DATE,
		{{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}.ASSESSMENT_DATE,
		{{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}.NCPR_FLAG,
		{{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}.CAMPAIGN_CODE,
		{{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}.FHB_FLAG,
		{{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}.SETTLEMENT_DATE,
		{{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}.PEXA_FLAG,
		{{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}.HSCA_FLAG,
		{{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}.HSCA_CONVERTED_TO_FULL_AT,
		{{ ref('CpyRejtHlAppRejectOra') }}.HL_APP_ID AS HL_APP_ID_R,
		{{ ref('CpyRejtHlAppRejectOra') }}.HL_APP_PROD_ID AS HL_APP_PROD_ID_R,
		{{ ref('CpyRejtHlAppRejectOra') }}.HL_PACKAGE_CAT_ID_R,
		{{ ref('CpyRejtHlAppRejectOra') }}.LPC_OFFICE_R,
		{{ ref('CpyRejtHlAppRejectOra') }}.STATUS_TRACKER_ID_R,
		{{ ref('CpyRejtHlAppRejectOra') }}.CHL_APP_PCD_EXT_SYS_CAT_ID_R AS CHL_APP_PCD_EXTERNAL_SYS_CAT_ID_R,
		{{ ref('CpyRejtHlAppRejectOra') }}.CHL_APP_SIMPLE_APP_FLAG_R,
		{{ ref('CpyRejtHlAppRejectOra') }}.CHL_APP_ORIGINATING_AGENT_ID_R,
		{{ ref('CpyRejtHlAppRejectOra') }}.CHL_APP_AGENT_NAME_R,
		{{ ref('CpyRejtHlAppRejectOra') }}.CASS_WITHHOLD_RISKBANK_FLAG_R,
		{{ ref('CpyRejtHlAppRejectOra') }}.ORIG_ETL_D_R,
		{{ ref('CpyRejtHlAppRejectOra') }}.CR_DATE_R,
		{{ ref('CpyRejtHlAppRejectOra') }}.ASSESSMENT_DATE_R,
		{{ ref('CpyRejtHlAppRejectOra') }}.NCPR_FLAG_R,
		{{ ref('CpyRejtHlAppRejectOra') }}.FHB_FLAG_R,
		{{ ref('CpyRejtHlAppRejectOra') }}.SETTLEMENT_DATE_R,
		{{ ref('CpyRejtHlAppRejectOra') }}.PEXA_FLAG_R,
		{{ ref('CpyRejtHlAppRejectOra') }}.HSCA_FLAG_R,
		{{ ref('CpyRejtHlAppRejectOra') }}.HSCA_CONVERTED_TO_FULL_AT_R
	FROM {{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}
	OUTER JOIN {{ ref('CpyRejtHlAppRejectOra') }} ON {{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}.HL_APP_ID = {{ ref('CpyRejtHlAppRejectOra') }}.HL_APP_ID
	AND {{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}.HL_APP_PROD_ID = {{ ref('CpyRejtHlAppRejectOra') }}.HL_APP_PROD_ID
)

SELECT * FROM JoinSrcSortReject