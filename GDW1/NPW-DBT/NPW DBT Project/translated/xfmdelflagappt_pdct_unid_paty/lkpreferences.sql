{{ config(materialized='view', tags=['XfmDelFlagAPPT_PDCT_UNID_PATY']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('XfmProsKey') }}.DELETED_TABLE_NAME,
		{{ ref('XfmProsKey') }}.DELETED_KEY_1_VALUE,
		{{ ref('SrcLdPROS_KEY_HASHLks') }}.PROS_KEY_I
	FROM {{ ref('XfmProsKey') }}
	LEFT JOIN {{ ref('SrcLdPROS_KEY_HASHLks') }} ON 
)

SELECT * FROM LkpReferences