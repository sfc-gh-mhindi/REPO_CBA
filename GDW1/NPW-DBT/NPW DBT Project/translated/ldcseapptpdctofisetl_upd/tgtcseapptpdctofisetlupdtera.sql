{{ config(materialized='incremental', alias='cse_appt_pdct_ofi_setl', incremental_strategy='merge', tags=['LdCseApptPdctOfiSetl_Upd']) }}

SELECT
	APPT_PDCT_I
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtCseApptPdctOfiSetlUpdateDS') }}