{{ config(materialized='view', tags=['XfmEvntRel']) }}

WITH Sort AS (
	SELECT
		MOD_TIMESTAMP,
		OL_CLIENT_RM_RATING_ID,
		CLIENT_ID,
		CIF_CODE,
		OU_ID,
		CS_USER_ID,
		RATING,
		WIM_PROCESS_ID
	FROM {{ ref('Funnel') }}
	ORDER BY OL_CLIENT_RM_RATING_ID ASC
)

SELECT * FROM Sort