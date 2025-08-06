{{ config(materialized='incremental', alias='appt_pdct_rel', incremental_strategy='merge', tags=['LdApptPdctRelUpd']) }}

SELECT
	APPT_PDCT_I
	RELD_APPT_PDCT_I
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptPdctRelUpdateDS') }}