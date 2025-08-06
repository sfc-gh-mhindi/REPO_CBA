{{ config(materialized='view', tags=['XfmAppt']) }}

WITH Lkp AS (
	SELECT
		{{ ref('Filter') }}.APP_ID,
		{{ ref('Filter') }}.CHANNEL_CAT_ID,
		{{ ref('Filter') }}.APP_NO,
		{{ ref('Filter') }}.CREATED_DATE,
		{{ ref('Filter') }}.APP_ENTRY_POINT,
		{{ ref('MAP_CSE_APPT_ORIG') }}.APPT_ORIG_C
	FROM {{ ref('Filter') }}
	LEFT JOIN {{ ref('MAP_CSE_APPT_ORIG') }} ON {{ ref('Filter') }}.CHANNEL_CAT_ID = {{ ref('MAP_CSE_APPT_ORIG') }}.CHNL_CAT_ID
	WHERE CHANNEL_CAT_ID_CHK = 'N'
)

SELECT * FROM Lkp