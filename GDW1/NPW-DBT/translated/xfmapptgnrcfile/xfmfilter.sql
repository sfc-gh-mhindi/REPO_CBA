{{ config(materialized='view', tags=['XfmApptGnrcFile']) }}

WITH XfmFilter AS (
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
	FROM {{ ref('LeftJoinEvnt') }}
	WHERE {{ ref('LeftJoinEvnt') }}.dummy = 0
)

SELECT * FROM XfmFilter