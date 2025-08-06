{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__hlmapp__cse__appt__pdct__ofi__setl', incremental_strategy='insert_overwrite', tags=['XfmHlmapp_Tpb']) }}

SELECT
	APPT_PDCT_I,
	OFI_IDNN_X,
	OFI_M,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	ROW_SECU_ACCS_C,
	RUN_STREAM 
FROM {{ ref('XfmTrans__outCSE_APPT_PDCT_OFI_SETL') }}