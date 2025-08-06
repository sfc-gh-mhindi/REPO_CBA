{{ config(materialized='view', tags=['DltAppt_Pdct_AmtFrmTMP']) }}

WITH gdw_cpy AS (
	SELECT
		APPT_PDCT_I,
		AMT_TYPE_C,
		CNCY_C,
		APPT_PDCT_A,
		XCES_AMT_REAS_X,
		SRCE_SYST_C,
		EFFT_D
	FROM {{ ref('Appt_Pdct_Amt_Tgt') }}
)

SELECT * FROM gdw_cpy