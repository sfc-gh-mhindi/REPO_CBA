{{ config(materialized='view', tags=['XfmApptGnrcFile']) }}

WITH Sort AS (
	SELECT
		APPT_I,
		DATE_ROLE_C,
		GNRC_ROLE_S,
		GNRC_ROLE_D,
		GNRC_ROLE_T,
		MODF_S,
		MODF_D,
		MODF_T,
		USER_I,
		CHNG_REAS_TYPE_C,
		promise_type
	FROM {{ ref('XfmFilter') }}
	ORDER BY APPT_I ASC, promise_type ASC, MODF_S ASC
)

SELECT * FROM Sort