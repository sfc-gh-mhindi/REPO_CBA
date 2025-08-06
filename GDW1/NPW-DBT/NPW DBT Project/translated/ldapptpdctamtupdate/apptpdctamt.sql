{{ config(materialized='incremental', alias='appt_pdct_amt', incremental_strategy='merge', tags=['LdApptPdctAmtUpdate']) }}

SELECT
	APPT_PDCT_I
	AMT_TYPE_C
	EFFT_D
	SRCE_SYST_C
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('ApptPdctAmtUpdate') }}