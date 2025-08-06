{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_phys__adrs__i__cse__ccl__bus__app__fee__20060616', incremental_strategy='insert_overwrite', tags=['DltASET_ADRSFrmTMP_ASET_ADRS']) }}

SELECT
	ASET_I,
	ADRS_I,
	SRCE_SYST_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptFeatInsertDS') }}