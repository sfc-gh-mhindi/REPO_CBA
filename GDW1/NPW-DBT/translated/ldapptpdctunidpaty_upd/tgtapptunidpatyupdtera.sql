{{ config(materialized='incremental', alias='appt_pdct_unid_paty', incremental_strategy='merge', tags=['LdApptPdctUnidPaty_Upd']) }}

SELECT
	APPT_PDCT_I
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptUnidPatyUpdateDS') }}