{{ config(materialized='view', tags=['XfmDelFlagAPPT_PDCT_FEAT']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('XfmProsKey') }}.DELETED_TABLE_NAME,
		{{ ref('XfmProsKey') }}.DELETED_KEY_1_VALUE,
		{{ ref('XfmProsKey') }}.DELETED_KEY_2_VALUE,
		{{ ref('XfmProsKey') }}.DELETED_KEY_3_VALUE,
		{{ ref('XfmProsKey') }}.APPT_PDCT_I,
		{{ ref('XfmProsKey') }}.SRCE_SYST_APPT_FEAT_I,
		{{ ref('SrcMAP_CSE_APP_PROD_EXCLLks') }}.DUMMY_PDCT_F,
		{{ ref('SrcLdPROS_KEY_HASHLks') }}.PROS_KEY_I
	FROM {{ ref('XfmProsKey') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_APP_PROD_EXCLLks') }} ON 
	LEFT JOIN {{ ref('SrcLdPROS_KEY_HASHLks') }} ON 
)

SELECT * FROM LkpReferences