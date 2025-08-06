{{ config(materialized='view', tags=['DltAppt_Pdct_RpayFrmTMP']) }}

WITH src_cpy AS (
	SELECT
		APPT_PDCT_I,
		SRCE_SYST_C,
		RPAY_SRCE_C,
		RPAY_SRCE_OTHR_X,
		RPAY_TYPE_C,
		PAYT_FREQ_C,
		STRT_RPAY_D,
		EROR_SEQN_I
	FROM {{ ref('XfmApptPdctRpay') }}
)

SELECT * FROM src_cpy