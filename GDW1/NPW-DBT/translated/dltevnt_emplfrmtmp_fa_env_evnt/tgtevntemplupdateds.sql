{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_evnt__empl__u__cse__coi__bus__envi__evnt__20061016', incremental_strategy='insert_overwrite', tags=['DltEVNT_EMPLFrmTMP_FA_ENV_EVNT']) }}

SELECT
	EVNT_I,
	EMPL_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtEvntEmplUpdateDS') }}