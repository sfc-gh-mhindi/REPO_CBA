{{ config(materialized='incremental', alias='acct_appt_pdct', incremental_strategy='merge', tags=['LdAcctApptPdct_Upd']) }}

SELECT
	APPT_PDCT_I
	ACCT_I
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtAcctApptUpdateDS') }}