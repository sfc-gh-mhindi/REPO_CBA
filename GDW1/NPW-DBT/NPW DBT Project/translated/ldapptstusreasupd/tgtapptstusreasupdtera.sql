{{ config(materialized='incremental', alias='appt_pdct_acct', incremental_strategy='merge', tags=['LdApptStusReasUpd']) }}

SELECT
	APPT_I
	STUS_C
	STUS_REAS_TYPE_C
	STRT_S
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptStusReasUpdateDS') }}