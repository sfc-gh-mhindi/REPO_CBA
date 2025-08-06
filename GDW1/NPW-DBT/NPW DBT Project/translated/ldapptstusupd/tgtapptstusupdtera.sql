{{ config(materialized='incremental', alias='appt_pdct_acct', incremental_strategy='merge', tags=['LdApptStusUpd']) }}

SELECT
	APPT_I
	STUS_C
	STRT_S
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptStusUpdateDS') }}