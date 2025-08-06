{{ config(materialized='incremental', alias='cse_appt_cpgn', incremental_strategy='merge', tags=['LdCseApptCpgnUpd']) }}

SELECT
	APPT_I
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtCseApptCpgnUpdateDS') }}