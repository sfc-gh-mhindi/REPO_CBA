{{ config(materialized='view', tags=['XfmDelFlagACCT_APPT_PDCT']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('XfmProsKey') }}.DLTD_TABL_NAME AS DELETED_TABLE_NAME,
		{{ ref('XfmProsKey') }}.DLTD_KEY1_VALU,
		{{ ref('XfmProsKey') }}.APPT_PDCT_I,
		{{ ref('XfmProsKey') }}.ACCT_I,
		{{ ref('SrcMAP_CSE_APP_PROD_EXCLLks') }}.DUMMY_PDCT_F,
		{{ ref('SrcLdPROS_KEY_HASHLks') }}.PROS_KEY_I
	FROM {{ ref('XfmProsKey') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_APP_PROD_EXCLLks') }} ON 
	LEFT JOIN {{ ref('SrcLdPROS_KEY_HASHLks') }} ON 
)

SELECT * FROM LkpReferences