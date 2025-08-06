{{ config(materialized='incremental', alias='appt_pdct_chkl', incremental_strategy='append', tags=['LdApptPdctChklIns']) }}

SELECT
	APPT_PDCT_I
	CHKL_ITEM_C
	STUS_D
	STUS_C
	SRCE_SYST_C
	CHKL_ITEM_X
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptPdctChklInsertDS') }}