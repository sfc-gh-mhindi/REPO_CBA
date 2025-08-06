{{ config(materialized='view', tags=['XfmDelFlagAPPT_TRNF_DETL']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('XfmProsKey') }}.APPT_I,
		{{ ref('XfmProsKey') }}.DELETED_KEY_1_VALUE,
		{{ ref('SrcLdPROS_KEY_HASHLks') }}.PROS_KEY_I
	FROM {{ ref('XfmProsKey') }}
	LEFT JOIN {{ ref('SrcLdPROS_KEY_HASHLks') }} ON 
)

SELECT * FROM LkpReferences