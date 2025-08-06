{{ config(materialized='view', tags=['XfmAppt']) }}

WITH Filter AS (
	SELECT
		APP_ID,
		CHANNEL_CAT_ID,
		APP_NO,
		CREATED_DATE,
		APP_ENTRY_POINT,
		APP_ID,
		CHANNEL_CAT_ID,
		APP_NO,
		CREATED_DATE,
		APP_ENTRY_POINT,
		APPT_ORIG_C
	FROM {{ ref('XfmNull') }}
)

SELECT * FROM Filter