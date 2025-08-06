{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_unid__paty__gnrc__i__cse__chl__bus__app__20100914', incremental_strategy='insert_overwrite', tags=['DltUNID_PATY_GNRCFrmTMP_UNID_PATY_GNRC']) }}

SELECT
	UNID_PATY_I,
	EFFT_D,
	PATY_TYPE_C,
	PATY_ROLE_C,
	PROS_KEY_EFFT_I,
	SRCE_SYST_C,
	PATY_QLFY_C,
	SRCE_SYST_PATY_I 
FROM {{ ref('Xfm') }}