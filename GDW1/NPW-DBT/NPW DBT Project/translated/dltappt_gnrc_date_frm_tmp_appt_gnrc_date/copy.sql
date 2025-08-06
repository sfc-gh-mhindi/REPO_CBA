{{ config(materialized='view', tags=['DltAPPT_GNRC_DATE_Frm_TMP_APPT_GNRC_DATE']) }}

WITH Copy AS (
	SELECT
		APPT_I,
		GNRC_ROLE_D,
		EFFT_D
	FROM {{ ref('APPT_GNRC_DATE') }}
)

SELECT * FROM Copy