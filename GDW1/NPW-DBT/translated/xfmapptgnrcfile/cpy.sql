{{ config(materialized='view', tags=['XfmApptGnrcFile']) }}

WITH Cpy AS (
	SELECT
		APPT_I,
		MODF_S,
		promise_type,
		DATE_ROLE_C,
		GNRC_ROLE_S,
		GNRC_ROLE_D,
		GNRC_ROLE_T,
		MODF_D,
		MODF_T,
		USER_I,
		CHNG_REAS_TYPE_C
	FROM {{ ref('Sort') }}
)

SELECT * FROM Cpy