{{ config(materialized='incremental', alias='appt_pdct_acct', incremental_strategy='merge', tags=['LdCseApptHlCondApprUpd']) }}

SELECT
	APPT_I
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptHlCondApprUpdateDS') }}