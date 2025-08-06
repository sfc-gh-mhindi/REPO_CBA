{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_adrs__i__cse__chl__bus__prty__adrs__20110701', incremental_strategy='insert_overwrite', tags=['DltADRSFrmTMP_ADRS']) }}

SELECT
	ADRS_I,
	ADRS_TYPE_C,
	SRCE_SYST_C,
	ADRS_QLFY_C,
	SRCE_SYST_ADRS_I,
	SRCE_SYST_ADRS_SEQN_N,
	EFFT_D,
	PROS_KEY_EFFT_I,
	ROW_SECU_ACCS_C 
FROM {{ ref('Split__InTmpADRSTera') }}