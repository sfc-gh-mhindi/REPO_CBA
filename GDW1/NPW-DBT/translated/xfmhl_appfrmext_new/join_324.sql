{{ config(materialized='view', tags=['XfmHL_APPFrmExt_NEW']) }}

WITH Join_324 AS (
	SELECT
		{{ ref('SrcHlAppPremapDS') }}.HL_APP_ID,
		{{ ref('SrcHlAppPremapDS') }}.HL_APP_PROD_ID,
		{{ ref('SrcHlAppPremapDS') }}.HL_PACKAGE_CAT_ID,
		{{ ref('SrcHlAppPremapDS') }}.LPC_OFFICE,
		{{ ref('SrcHlAppPremapDS') }}.STATUS_TRACKER_ID,
		{{ ref('SrcHlAppPremapDS') }}.CHL_APP_PCD_EXT_SYS_CAT_ID,
		{{ ref('SrcHlAppPremapDS') }}.CHL_APP_SIMPLE_APP_FLAG,
		{{ ref('SrcHlAppPremapDS') }}.CHL_APP_ORIGINATING_AGENT_ID,
		{{ ref('SrcHlAppPremapDS') }}.CHL_APP_AGENT_NAME,
		{{ ref('SrcHlAppPremapDS') }}.CASS_WITHHOLD_RISKBANK_FLAG,
		{{ ref('SrcHlAppPremapDS') }}.CR_DATE,
		{{ ref('SrcHlAppPremapDS') }}.ASSESSMENT_DATE,
		{{ ref('SrcHlAppPremapDS') }}.NCPR_FLAG,
		{{ ref('SrcHlAppPremapDS') }}.CAMPAIGN_CODE,
		{{ ref('SrcHlAppPremapDS') }}.FHB_FLAG,
		{{ ref('SrcHlAppPremapDS') }}.SETTLEMENT_DATE,
		{{ ref('SrcHlAppPremapDS') }}.ORIG_ETL_D,
		{{ ref('CpyHlAppSeq') }}.FOREIGN_INCOME_FLAG,
		{{ ref('CpyHlAppSeq') }}.FI_CURRENCY_CODE
	FROM {{ ref('SrcHlAppPremapDS') }}
	LEFT JOIN {{ ref('CpyHlAppSeq') }} ON {{ ref('SrcHlAppPremapDS') }}.HL_APP_ID = {{ ref('CpyHlAppSeq') }}.HL_APP_ID
)

SELECT * FROM Join_324