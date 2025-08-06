{{ config(materialized='view', tags=['XfmDelFlagAPPT_PDCT_ACCT']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('XfmProsKey') }}.DELETED_TABLE_NAME,
		{{ ref('XfmProsKey') }}.APP_PROD_ID,
		{{ ref('SrcMAP_CSE_APP_PROD_EXCLLks') }}.DUMMY_PDCT_F,
		{{ ref('SrcLdPROS_KEY_HASHLks') }}.PROS_KEY_I
	FROM {{ ref('XfmProsKey') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_APP_PROD_EXCLLks') }} ON 
	LEFT JOIN {{ ref('SrcLdPROS_KEY_HASHLks') }} ON 
)

SELECT * FROM LkpReferences