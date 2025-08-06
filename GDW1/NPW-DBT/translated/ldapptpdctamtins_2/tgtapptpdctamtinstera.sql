{{ config(materialized='incremental', alias='appt_pdct_amt', incremental_strategy='append', tags=['LdApptPdctAmtIns_2']) }}

SELECT
	APPT_PDCT_I
	AMT_TYPE_C
	EFFT_D
	EXPY_D
	CNCY_C
	APPT_PDCT_A
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('TgtApptPdctAmtInsertDS') }}