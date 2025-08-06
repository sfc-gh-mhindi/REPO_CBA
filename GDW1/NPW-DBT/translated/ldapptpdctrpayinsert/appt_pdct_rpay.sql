{{ config(materialized='incremental', alias='appt_pdct_paty', incremental_strategy='append', tags=['LdApptPdctRpayInsert']) }}

SELECT
	APPT_PDCT_I
	RPAY_TYPE_C
	EFFT_D
	PAYT_FREQ_C
	STRT_RPAY_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I
	SRCE_SYST_C
	RPAY_SRCE_C
	RPAY_SRCE_OTHR_X 
FROM {{ ref('ApptPdctRpayInsert') }}