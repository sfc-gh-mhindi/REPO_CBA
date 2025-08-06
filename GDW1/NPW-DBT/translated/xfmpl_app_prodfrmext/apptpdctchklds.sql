{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_tmp__cse__cpl__bus__app__prod__appt__pdct__chkl', incremental_strategy='insert_overwrite', tags=['XfmPL_APP_PRODFrmExt']) }}

SELECT
	APPT_PDCT_I,
	CHKL_ITEM_C,
	STUS_D,
	STUS_C,
	SRCE_SYST_C,
	CHKL_ITEM_X,
	RUN_STRM 
FROM {{ ref('OutApptPdctChklDS') }}