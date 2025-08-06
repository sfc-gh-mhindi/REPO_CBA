{{ config(materialized='view', tags=['XfmCPL_APP_PROD_Appt_Pdct_FeatExt']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('CpyRename') }}.APP_PROD_ID,
		{{ ref('TgtMAP_CSE_APPT_PDCT_FEATLks') }}.CAMPAIGN_CAT_ID,
		{{ ref('CpyRename') }}.ORIG_ETL_D,
		{{ ref('SrcMAP_CSE_APPT_QLFYLks') }}.APPT_QLFY_C,
		{{ ref('TgtMAP_CSE_APPT_PDCT_FEATLks') }}.FEAT_I,
		{{ ref('TgtMAP_CSE_APPT_PDCT_FEATLks') }}.ACTL_VAL_R
	FROM {{ ref('CpyRename') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_APPT_QLFYLks') }} ON 
	LEFT JOIN {{ ref('TgtMAP_CSE_APPT_PDCT_FEATLks') }} ON 
)

SELECT * FROM LkpReferences