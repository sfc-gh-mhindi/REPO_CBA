{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_paty__rel__u__cse__coi__bus__clnt__undtak__20110707', incremental_strategy='insert_overwrite', tags=['DltPATY_RELFrmTMP_FA_CLIENT_UNDERTAKING2']) }}

SELECT
	REL_I,
	PATY_I,
	RELD_PATY_I,
	REL_LEVL_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__TgtPatyRelUpdatetDS') }}