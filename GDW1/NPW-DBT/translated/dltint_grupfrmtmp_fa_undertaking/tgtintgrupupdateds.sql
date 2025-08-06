{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_int__grup__u__cse__coi__bus__undtak__20060101', incremental_strategy='insert_overwrite', tags=['DltINT_GRUPFrmTMP_FA_UNDERTAKING']) }}

SELECT
	INT_GRUP_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtIntGrupUpdateDS') }}