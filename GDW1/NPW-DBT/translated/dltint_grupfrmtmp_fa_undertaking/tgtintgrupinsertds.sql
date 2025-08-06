{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_int__grup__i__cse__coi__bus__undtak__20060101', incremental_strategy='insert_overwrite', tags=['DltINT_GRUPFrmTMP_FA_UNDERTAKING']) }}

SELECT
	INT_GRUP_I,
	INT_GRUP_TYPE_C,
	INT_GRUP_M,
	SRCE_SYST_INT_GRUP_I,
	SRCE_SYST_C,
	ORIG_SRCE_SYST_INT_GRUP_I,
	CRAT_D,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtIntGrupInsertDS') }}