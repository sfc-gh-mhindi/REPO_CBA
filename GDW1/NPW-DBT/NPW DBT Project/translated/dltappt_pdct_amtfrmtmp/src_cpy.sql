{{ config(materialized='view', tags=['DltAppt_Pdct_AmtFrmTMP']) }}

WITH src_cpy AS (
	SELECT
		APPT_PDCT_I,
		AMT_TYPE_C,
		CNCY_C,
		APPT_PDCT_A,
		XCES_AMT_REAS_X,
		SRCE_SYST_C,
		EROR_SEQN_I
	FROM {{ ref('XfmApptPdctAmt') }}
)

SELECT * FROM src_cpy