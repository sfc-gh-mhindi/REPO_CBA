{{ config(materialized='view', tags=['LdTMP_APPT_ASETFrmXfm1']) }}

WITH Cpy AS (
	SELECT
		APPT_I,
		ASET_I,
		PRIM_SECU_F,
		RUN_STRM,
		ASET_SETL_REQD
	FROM {{ ref('TgtTmp_ApptAsetDS') }}
)

SELECT * FROM Cpy