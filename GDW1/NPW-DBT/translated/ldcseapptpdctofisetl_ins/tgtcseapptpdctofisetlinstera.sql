{{ config(materialized='incremental', alias='cse_appt_pdct_ofi_setl', incremental_strategy='append', tags=['LdCseApptPdctOfiSetl_Ins']) }}

SELECT
	APPT_PDCT_I
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	DCHG_OFI_IDNN_X
	DCHG_OFI_M
	ROW_SECU_ACESS_C 
FROM {{ ref('TgtCseApptPdctOfiSetlInsertDS') }}