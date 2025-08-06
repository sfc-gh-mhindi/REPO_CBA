{{ config(materialized='incremental', alias='appt_gnrc_date', incremental_strategy='merge', tags=['LdApptGnrcDatechUpd']) }}

SELECT
	APPT_I
	DATE_ROLE_C
	EXPY_D
	PROS_KEY_EXPY_I
	EFFT_D 
FROM {{ ref('APPT_GNRC_DATE_U') }}