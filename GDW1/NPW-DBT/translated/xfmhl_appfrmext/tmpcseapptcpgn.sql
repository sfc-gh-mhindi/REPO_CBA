{{ config(materialized='incremental', alias='_cba__app_pj__rapidresponseteam_csel4_dev_dataset_tmp__cse__chl__bus__app__cse__appt__cpgn', incremental_strategy='insert_overwrite', tags=['XfmHL_APPFrmExt']) }}

SELECT
	APPT_I,
	CSE_CPGN_CODE_X,
	EFFT_D,
	RUN_STRM 
FROM {{ ref('DeDup_CseApptCpgn') }}