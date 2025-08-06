{{ config(materialized='incremental', alias='paty_rel', incremental_strategy='append', tags=['LdPatyRelIns']) }}

SELECT
	PATY_I
	RELD_PATY_I
	REL_I
	REL_TYPE_C
	SRCE_SYST_C
	PATY_ROLE_C
	REL_STUS_C
	REL_REAS_C
	REL_LEVL_C
	REL_EFFT_D
	REL_EXPY_D
	SRCE_SYST_REL_I
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	ROW_SECU_ACCS_C 
FROM {{ ref('TgtPatyRelInsertDS') }}