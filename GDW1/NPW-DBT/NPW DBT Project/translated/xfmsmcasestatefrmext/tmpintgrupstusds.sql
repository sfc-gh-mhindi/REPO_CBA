{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__sm__case__state__int__grup__stus', incremental_strategy='insert_overwrite', tags=['XfmSmCaseStateFrmExt']) }}

SELECT
	INT_GRUP_I,
	STRT_S,
	STUS_C,
	STRT_D,
	STRT_T,
	EMPL_I,
	END_S,
	END_D,
	END_T,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	RUN_STRM 
FROM {{ ref('XfmBusinessRules__OutTmpIntGrupStusDS') }}