{{ config(materialized='view', tags=['LdTMP_APPT_ACTVFrmXfm']) }}

WITH Cpy AS (
	SELECT
		APPT_I,
		APPT_ACTV_Q,
		RUN_STRM
	FROM {{ ref('TgtTmpApptActvDS') }}
)

SELECT * FROM Cpy