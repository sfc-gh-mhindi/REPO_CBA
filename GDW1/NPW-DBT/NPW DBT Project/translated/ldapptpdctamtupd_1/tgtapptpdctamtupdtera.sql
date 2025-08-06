{{ config(materialized='incremental', alias='appt_pdct_amt', incremental_strategy='merge', tags=['LdApptPdctAmtUpd_1']) }}

SELECT
	APPT_PDCT_I
	AMT_TYPE_C
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptPdctAmtUpdateDS') }}