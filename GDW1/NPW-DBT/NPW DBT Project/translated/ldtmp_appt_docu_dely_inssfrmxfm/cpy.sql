{{ config(materialized='view', tags=['LdTMP_APPT_DOCU_DELY_INSSFrmXfm']) }}

WITH Cpy AS (
	SELECT
		APPT_I,
		DOCU_DELY_RECV_C,
		RUN_STRM
	FROM {{ ref('TgtTmpApptDocuDelyInssDS') }}
)

SELECT * FROM Cpy