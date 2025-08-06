{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_tmp__cse__ccl__bus__app__cse__cmln__appt__xpos__ases__detl', incremental_strategy='insert_overwrite', tags=['XfmCclBusAppFrmExt']) }}

SELECT
	APPT_I,
	XPOS_A,
	XPOS_AMT_D,
	OVRD_COVTS_ASES_F,
	CSE_CMLN_OVRD_REAS_CATG_C,
	SHRT_DFLT_OVRD_F,
	EFFT_D,
	EXPY_D 
FROM {{ ref('XfmBusinessRules__OutTmpCclCseCmlnApptXposAsesDetll') }}