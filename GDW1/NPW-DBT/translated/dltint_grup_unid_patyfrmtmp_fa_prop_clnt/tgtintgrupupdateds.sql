{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_int__grup__unid__paty__u__cse__coi__bus__prop__clnt__20060101', incremental_strategy='insert_overwrite', tags=['DltINT_GRUP_UNID_PATYFrmTMP_FA_PROP_CLNT']) }}

SELECT
	INT_GRUP_I,
	SRCE_SYST_PATY_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtIntGrupUpdateDS') }}