{{ config(materialized='incremental', alias='tmp_cse_appt_pdct_ofi_setl', incremental_strategy='append', tags=['LdTMP_CSE_APPT_PDCT_OFI_SETL_FrmXfm']) }}

SELECT
	APPT_PDCT_I
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	DCHG_OFI_IDNN_X
	DCHG_OFI_M
	ROW_SECU_ACESS_C
	RUN_STRM 
FROM {{ ref('Copy_89') }}