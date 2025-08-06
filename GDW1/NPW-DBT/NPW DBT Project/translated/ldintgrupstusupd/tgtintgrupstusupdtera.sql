{{ config(materialized='incremental', alias='appt_pdct_acct', incremental_strategy='merge', tags=['LdIntGrupStusUpd']) }}

SELECT
	INT_GRUP_I
	STRT_S
	STUS_C
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtIntGrupStusUpdateDS') }}