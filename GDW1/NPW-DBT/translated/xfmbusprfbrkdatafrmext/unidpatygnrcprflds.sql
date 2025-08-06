{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_tmp__cse__prd__bus__prf__brk__data__unid__paty__gnrc__prfl', incremental_strategy='insert_overwrite', tags=['XfmBusPrfBrkDataFrmExt']) }}

SELECT
	UNID_PATY_I,
	SRCE_SYST_C,
	GRDE_C,
	SUB_GRDE_C,
	PRNT_PRVG_F,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	ROW_SECU_ACCS_C 
FROM {{ ref('Xfmbrkdata__ToUnidPatyGnrcPrfl') }}