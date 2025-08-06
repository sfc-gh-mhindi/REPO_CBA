{{ config(materialized='view', tags=['XfmDelFlagAPPT_PDCT']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('XfmRename') }}.DLTD_TABL_NAME AS DELETED_TABLE_NAME,
		{{ ref('XfmRename') }}.DLTD_KEY1_VALU,
		{{ ref('XfmRename') }}.APPT_I,
		{{ ref('SrcMAP_CSE_APP_PROD_EXCLLks') }}.DUMMY_PDCT_F,
		{{ ref('SrcLdPROS_KEY_HASHLks') }}.PROS_KEY_I
	FROM {{ ref('XfmRename') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_APP_PROD_EXCLLks') }} ON 
	LEFT JOIN {{ ref('SrcLdPROS_KEY_HASHLks') }} ON 
)

SELECT * FROM LkpReferences