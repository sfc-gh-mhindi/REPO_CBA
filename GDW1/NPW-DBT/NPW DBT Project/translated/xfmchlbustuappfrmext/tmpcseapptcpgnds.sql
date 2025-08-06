{{ config(materialized='incremental', alias='_cba__app_hlt_dev_dataset_tmp__cse__chl__bus__tu__app__cse__appt__cpgn', incremental_strategy='insert_overwrite', tags=['XfmChlBusTuAPPFrmExt']) }}

SELECT
	APPT_I,
	CSE_CPGN_CODE_X,
	EFFT_D,
	RUN_STRM 
FROM {{ ref('Remove_Duplicates_TuAppid') }}