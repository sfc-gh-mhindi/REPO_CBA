{{ config(materialized='incremental', alias='appt_pdct_chkl', incremental_strategy='merge', tags=['LdApptPdctChklUpd']) }}

SELECT
	APPT_PDCT_I
	CHKL_ITEM_C
	SRCE_SYST_C
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptPdctChklUpdateDS') }}