{{ config(materialized='view', alias='appt_pdct_fndd_inss', tags=['LdAPPT_PDCT_FNDD_INSSUpd']) }}

SELECT
	APPT_PDCT_I
	APPT_PDCT_FNDD_I
	APPT_PDCT_FNDD_METH_I
	FNDD_INSS_METH_C
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtAPPT_PDCT_DOCU_DELY_INSSUpDS') }}