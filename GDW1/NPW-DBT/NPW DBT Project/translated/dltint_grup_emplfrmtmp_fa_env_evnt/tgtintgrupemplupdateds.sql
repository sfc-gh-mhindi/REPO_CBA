{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_int__grup__empl__u__cse__coi__bus__envi__evnt__20061016', incremental_strategy='insert_overwrite', tags=['DltINT_GRUP_EMPLFrmTMP_FA_ENV_EVNT']) }}

SELECT
	INT_GRUP_I,
	EMPL_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtIntGrupEmplUpdateDS') }}