{{ config(materialized='incremental', alias='tdcsodepo.unid_paty_gnrc', incremental_strategy='append', tags=['LdApptUnidPatyGnr_Ins']) }}

SELECT
	APPT_I
	EFFT_D
	EXPY_D
	REL_TYPE_C
	REL_REAS_C
	REL_STUS_C
	REL_LEVL_C
	SRCE_SYST_C
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	UNID_PATY_I
	ROW_SECU_ACCS_C 
FROM {{ ref('Transformer__LoadRows') }}