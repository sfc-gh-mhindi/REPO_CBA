{{ config(materialized='view', tags=['DltAppt_QstnFrmTMP']) }}

WITH gdw_cpy AS (
	SELECT
		APPT_I,
		QSTN_C,
		RESP_C,
		RESP_CMMT_X,
		PATY_I,
		EFFT_D
	FROM {{ ref('Appt_Qstn_Tgt') }}
)

SELECT * FROM gdw_cpy