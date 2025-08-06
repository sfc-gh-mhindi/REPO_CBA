{{ config(materialized='incremental', alias='unid_paty_gnrc_prfl', incremental_strategy='append', tags=['LdUnidPatyGnrcPrflIns']) }}

SELECT
	UNID_PATY_I
	SRCE_SYST_C
	GRDE_C
	SUB_GRDE_C
	PRNT_PRVG_F
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	ROW_SECU_ACCS_C 
FROM {{ ref('Cpy') }}