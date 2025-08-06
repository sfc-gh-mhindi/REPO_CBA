{{ config(materialized='view', tags=['DltApptGnrcDateCCL']) }}

WITH Copy AS (
	SELECT
		APPT_I,
		DATE_ROLE_C,
		GNRC_ROLE_S,
		MODF_S,
		USER_I,
		CHNG_REAS_TYPE_C,
		APPT_I,
		EFFT_D
	FROM {{ ref('APPT_GNRC_DATE') }}
)

SELECT * FROM Copy