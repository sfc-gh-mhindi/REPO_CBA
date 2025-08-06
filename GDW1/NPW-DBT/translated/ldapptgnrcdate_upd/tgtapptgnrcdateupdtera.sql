{{ config(materialized='incremental', alias='appt_pdct_unid_paty', incremental_strategy='merge', tags=['LdApptGnrcDate_Upd']) }}

SELECT
	APPT_I
	EXPY_D
	PROS_KEY_EXPY_I
	EFFT_D 
FROM {{ ref('TgtApptGnrcDateUpdateDS') }}