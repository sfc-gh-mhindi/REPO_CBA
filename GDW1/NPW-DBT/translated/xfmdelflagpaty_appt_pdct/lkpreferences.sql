{{ config(materialized='view', tags=['XfmDelFlagPATY_APPT_PDCT']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('XfmProsKey') }}.DLTD_TABL_NAME,
		{{ ref('XfmProsKey') }}.PATY_I,
		{{ ref('XfmProsKey') }}.APP_PROD_ID,
		{{ ref('XfmProsKey') }}.CIF_CODE,
		{{ ref('XfmProsKey') }}.ROLE_CAT_ID,
		{{ ref('XfmProsKey') }}.SBTY_CODE,
		{{ ref('XfmProsKey') }}.PATY_ROLE_C,
		{{ ref('SrcMAP_CSE_APP_PROD_EXCLLks') }}.DUMMY_PDCT_F,
		{{ ref('SrcMAP_CSE_APPT_QLFYLks') }}.APPT_QLFY_C,
		{{ ref('SrcLdPROS_KEY_HASHLks') }}.PROS_KEY_I
	FROM {{ ref('XfmProsKey') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_APP_PROD_EXCLLks') }} ON 
	LEFT JOIN {{ ref('SrcMAP_CSE_APPT_QLFYLks') }} ON 
	LEFT JOIN {{ ref('SrcLdPROS_KEY_HASHLks') }} ON 
)

SELECT * FROM LkpReferences