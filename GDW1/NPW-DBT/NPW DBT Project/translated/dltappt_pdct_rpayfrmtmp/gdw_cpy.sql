{{ config(materialized='view', tags=['DltAppt_Pdct_RpayFrmTMP']) }}

WITH gdw_cpy AS (
	SELECT
		APPT_PDCT_I,
		SRCE_SYST_C,
		RPAY_SRCE_C,
		RPAY_SRCE_OTHR_X,
		EFFT_D
	FROM {{ ref('Appt_Pdct_Rpay_Tgt') }}
)

SELECT * FROM gdw_cpy