{{ config(materialized='incremental', alias='appt_pdct_paty', incremental_strategy='merge', tags=['LdApptQstnUpdate']) }}

SELECT
	APPT_I
	QSTN_C
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('ApptQstnUpdate') }}