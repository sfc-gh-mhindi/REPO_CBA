{{ config(materialized='view', tags=['XfmDelFlagAPPT_PDCT_PATY']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('XfmProsKey') }}.DELETED_TABLE_NAME,
		{{ ref('XfmProsKey') }}.APP_PROD_ID,
		{{ ref('XfmProsKey') }}.CIF_CODE,
		{{ ref('XfmProsKey') }}.ROLE_CAT_ID,
		{{ ref('XfmProsKey') }}.SBTY_CODE AS SUBTYPE_CODE,
		{{ ref('SrcMAP_CSE_APP_PROD_EXCLLks') }}.DUMMY_PDCT_F,
		{{ ref('SrcMAP_CSE_APPT_QLFYLks') }}.APPT_QLFY_C,
		{{ ref('SrcLdMAP_CSE_APPT_PDCT_PATY_ROLELks') }}.PATY_ROLE_C,
		{{ ref('SrcLdPROS_KEY_HASHLks') }}.PROS_KEY_I
	FROM {{ ref('XfmProsKey') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_APP_PROD_EXCLLks') }} ON 
	LEFT JOIN {{ ref('SrcMAP_CSE_APPT_QLFYLks') }} ON 
	LEFT JOIN {{ ref('SrcLdMAP_CSE_APPT_PDCT_PATY_ROLELks') }} ON 
	LEFT JOIN {{ ref('SrcLdPROS_KEY_HASHLks') }} ON 
)

SELECT * FROM LkpReferences