{{ config(materialized='view', tags=['XfmHL_APPFrmExt']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('CpyRename') }}.HL_APP_ID,
		{{ ref('CpyRename') }}.HL_APP_PROD_ID,
		{{ ref('CpyRename') }}.HL_PACKAGE_CAT_ID,
		{{ ref('CpyRename') }}.LPC_OFFICE,
		{{ ref('CpyRename') }}.STATUS_TRACKER_ID,
		{{ ref('CpyRename') }}.CHL_APP_PCD_EXT_SYS_CAT_ID,
		{{ ref('CpyRename') }}.CHL_APP_SIMPLE_APP_FLAG,
		{{ ref('CpyRename') }}.CHL_APP_ORIGINATING_AGENT_ID,
		{{ ref('CpyRename') }}.CHL_APP_AGENT_NAME,
		{{ ref('CpyRename') }}.CASS_WITHHOLD_RISKBANK_FLAG,
		{{ ref('CpyRename') }}.CR_DATE,
		{{ ref('CpyRename') }}.ASSESSMENT_DATE,
		{{ ref('CpyRename') }}.NCPR_FLAG,
		{{ ref('CpyRename') }}.ORIG_ETL_D,
		{{ ref('SrcMAP_CSE_PACK_PDCT_HLLks') }}.PDCT_N,
		{{ ref('SrcMAP_CSE_LPC_DEPT_HLLks') }}.DEPT_I,
		{{ ref('CpyRename') }}.CAMPAIGN_CODE,
		{{ ref('CpyRename') }}.PEXA_FLAG,
		{{ ref('CpyRename') }}.HSCA_FLAG,
		{{ ref('CpyRename') }}.HSCA_CONVERTED_TO_FULL_AT,
		{{ ref('MAP_CSE_APPT_PDCT_FEAT') }}.FEAT_VALU_C
	FROM {{ ref('CpyRename') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_PACK_PDCT_HLLks') }} ON 
	LEFT JOIN {{ ref('SrcMAP_CSE_LPC_DEPT_HLLks') }} ON 
	LEFT JOIN {{ ref('MAP_CSE_APPT_PDCT_FEAT') }} ON {{ ref('CpyRename') }}.PEXA_FLAG = {{ ref('MAP_CSE_APPT_PDCT_FEAT') }}.PEXA_FLAG
)

SELECT * FROM LkpReferences