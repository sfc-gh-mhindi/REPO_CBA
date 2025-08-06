{{ config(materialized='view', tags=['DltAPPT_GNRC_DATE_Frm_TMP_APPT_GNRC_DATE']) }}

WITH Merge AS (
	SELECT
		{{ ref('Copy') }}.APPT_I,
		{{ ref('Cpy') }}.EXPY_D,
		{{ ref('Cpy') }}.PROS_KEY_EXPY_I,
		{{ ref('Copy') }}.EFFT_D
	FROM {{ ref('Copy') }}
	INNER JOIN {{ ref('Cpy') }} ON {{ ref('Copy') }}.APPT_I = {{ ref('Cpy') }}.APPT_I
)

SELECT * FROM Merge