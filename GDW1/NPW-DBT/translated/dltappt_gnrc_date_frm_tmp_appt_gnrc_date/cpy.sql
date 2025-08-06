{{ config(materialized='view', tags=['DltAPPT_GNRC_DATE_Frm_TMP_APPT_GNRC_DATE']) }}

WITH Cpy AS (
	SELECT
		APPT_I,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM {{ ref('DetermineChange__ToCpy') }}
)

SELECT * FROM Cpy