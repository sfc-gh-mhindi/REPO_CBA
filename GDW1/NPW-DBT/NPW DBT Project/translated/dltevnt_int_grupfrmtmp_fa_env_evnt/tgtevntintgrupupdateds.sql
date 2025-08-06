{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_evnt__int__grup__u__cse__coi__bus__envi__evnt__20060101', incremental_strategy='insert_overwrite', tags=['DltEVNT_INT_GRUPFrmTMP_FA_ENV_EVNT']) }}

SELECT
	INT_GRUP_I,
	EVNT_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtEvntIntGrupUpdateDS') }}