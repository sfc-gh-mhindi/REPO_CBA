{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_phys__adrs__u__cse__chl__bus__prty__adrs__20110701', incremental_strategy='insert_overwrite', tags=['DltPHYS_ADRSFrmTMP_PHYS_ADRS']) }}

SELECT
	ADRS_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptFeatUpdateDS') }}