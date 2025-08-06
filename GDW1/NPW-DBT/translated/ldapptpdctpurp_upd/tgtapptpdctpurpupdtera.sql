{{ config(materialized='incremental', alias='appt_pdct_purp', incremental_strategy='merge', tags=['LdApptPdctPurp_Upd']) }}

SELECT
	APPT_PDCT_I
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptPdctPurpUpdateDS') }}