{{ config(materialized='incremental', alias='appt_pdct_docu_dely_inss', incremental_strategy='merge', tags=['LdAPPT_PDCT_DOCU_DELY_INSSUpd']) }}

SELECT
	APPT_PDCT_I
	DOCU_DELY_METH_C
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtAPPT_PDCT_DOCU_DELY_INSSUpDS') }}