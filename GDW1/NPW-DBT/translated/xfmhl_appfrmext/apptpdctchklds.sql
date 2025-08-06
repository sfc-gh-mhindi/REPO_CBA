{{ config(materialized='incremental', alias='_cba__app_pj__rapidresponseteam_csel4_dev_dataset_tmp__cse__chl__bus__app__appt__pdct__chkl', incremental_strategy='insert_overwrite', tags=['XfmHL_APPFrmExt']) }}

SELECT
	APPT_PDCT_I,
	CHKL_ITEM_C,
	STUS_D,
	STUS_C,
	SRCE_SYST_C,
	CHKL_ITEM_X,
	RUN_STRM 
FROM {{ ref('XfmBusinessRules__OutApptPdctChklDS') }}