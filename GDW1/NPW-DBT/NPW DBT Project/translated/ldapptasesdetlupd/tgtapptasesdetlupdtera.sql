{{ config(materialized='incremental', alias='appt_pdct_acct', incremental_strategy='merge', tags=['LdApptAsesDetlUpd']) }}

SELECT
	APPT_I
	AMT_TYPE_C
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptAsesDetlUpdateDS') }}