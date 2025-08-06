{{ config(materialized='view', tags=['DltAppt_QstnFrmTMP']) }}

WITH src_cpy AS (
	SELECT
		APPT_I,
		QSTN_C,
		ROW_SECU_ACCS_C,
		EROR_SEQN_I,
		RESP_C,
		RESP_CMMT_X,
		PATY_I
	FROM {{ ref('XfmApptQstn') }}
)

SELECT * FROM src_cpy