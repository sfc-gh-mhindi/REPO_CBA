{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_unid__paty__gnrc__prfl__u__cse__chl__bus__app__20100914', incremental_strategy='insert_overwrite', tags=['DltUNID_PATY_GNRC_PRFLFrmTMP_UNID_PATY_GNRC_PRFL']) }}

SELECT
	UNID_PATY_I,
	EXPY_D,
	PROS_KEY_EXPY_I,
	EFFT_D 
FROM {{ ref('JoinToUpdates') }}