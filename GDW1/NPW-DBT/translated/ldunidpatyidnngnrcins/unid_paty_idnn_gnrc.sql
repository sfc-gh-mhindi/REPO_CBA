{{ config(materialized='incremental', alias='tdcsodepo.unid_paty_idnn_gnrc', incremental_strategy='append', tags=['LdUnidPatyIdnnGnrcIns']) }}

SELECT
	SRCE_SYST_C
	IDNN_TYPE_C
	IDNN_VALU_X
	EFFT_D
	EXPY_D
	UNID_PATY_I
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	ROW_SECU_ACCS_C 
FROM {{ ref('Transformer__LoadRows') }}