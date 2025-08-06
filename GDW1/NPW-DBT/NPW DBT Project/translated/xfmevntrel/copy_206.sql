{{ config(materialized='view', tags=['XfmEvntRel']) }}

WITH Copy_206 AS (
	SELECT
		MOD_TIMESTAMP,
		OL_CLIENT_RM_RATING_ID,
		CLIENT_ID,
		CIF_CODE,
		OU_ID,
		CS_USER_ID,
		RATING,
		WIM_PROCESS_ID
	FROM {{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}
)

SELECT * FROM Copy_206