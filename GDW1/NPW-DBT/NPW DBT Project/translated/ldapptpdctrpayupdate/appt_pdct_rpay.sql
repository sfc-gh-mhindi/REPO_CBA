{{ config(materialized='incremental', alias='appt_pdct_rpay', incremental_strategy='merge', tags=['LdApptPdctRpayUpdate']) }}

SELECT
	APPT_PDCT_I
	SRCE_SYST_C
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('ApptPdctRpayUpdate') }}