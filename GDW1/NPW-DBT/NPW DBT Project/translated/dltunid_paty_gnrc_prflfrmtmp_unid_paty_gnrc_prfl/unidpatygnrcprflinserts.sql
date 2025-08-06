{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_unid__paty__gnrc__prfl__i__cse__chl__bus__app__20100914', incremental_strategy='insert_overwrite', tags=['DltUNID_PATY_GNRC_PRFLFrmTMP_UNID_PATY_GNRC_PRFL']) }}

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
FROM {{ ref('DetermineChange__ToInserts') }}