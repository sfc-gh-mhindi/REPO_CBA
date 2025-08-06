{{ config(materialized='incremental', alias='appt_pdct_cpgn', incremental_strategy='merge', tags=['LdApptPdctCpgnUpd']) }}

SELECT
	APPT_PDCT_I
	CPGN_TYPE_C
	SRCE_SYST_C
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptPdctCpgnUpdateDS') }}