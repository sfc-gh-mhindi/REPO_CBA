{{ config(materialized='incremental', alias='appt_empl', incremental_strategy='merge', tags=['LdApptEmplUpd']) }}

SELECT
	EMPL_I
	APPT_I
	EMPL_ROLE_C
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptEmplUpdateDS') }}