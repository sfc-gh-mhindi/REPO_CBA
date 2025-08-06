{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_paty__int__grup__u__cse__coi__bus__clnt__undtak__20060824', incremental_strategy='insert_overwrite', tags=['DltPATY_INT_GRUPFrmTMP_FA_CLIENT_UNDERTAKING']) }}

SELECT
	INT_GRUP_I,
	SRCE_SYST_PATY_INT_GRUP_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__TgtPatyIntGrupUpdatetDS') }}