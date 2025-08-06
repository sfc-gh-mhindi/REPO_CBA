{{ config(materialized='incremental', alias='appt_gnrc_date', incremental_strategy='merge', tags=['LdApptGnrcDateUpd']) }}

SELECT
	APPT_I
	EXPY_D
	PROS_KEY_EXPY_I
	EFFT_D 
FROM {{ ref('TgtApptGnrcDateUpdateDS') }}