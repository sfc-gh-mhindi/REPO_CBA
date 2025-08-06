{{ config(materialized='view', tags=['XfmDelFlagPATY_INT_GRUP']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('XfmProsKey') }}.DELETED_TABLE_NAME,
		{{ ref('XfmProsKey') }}.APP_PROD_ID,
		{{ ref('XfmProsKey') }}.DELETED_KEY_2_VALUE,
		{{ ref('SrcLdPROS_KEY_HASHLks') }}.PROS_KEY_I
	FROM {{ ref('XfmProsKey') }}
	LEFT JOIN {{ ref('SrcLdPROS_KEY_HASHLks') }} ON 
)

SELECT * FROM LkpReferences