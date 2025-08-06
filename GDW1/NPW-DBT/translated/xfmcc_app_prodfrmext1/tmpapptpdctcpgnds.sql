{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_tmp__cse__ccc__bus__app__prod__appt__pdct__cpgn', incremental_strategy='insert_overwrite', tags=['XfmCC_APP_PRODFrmExt1']) }}

SELECT
	APPT_PDCT_I,
	CPGN_TYPE_C,
	CPGN_I,
	REL_C,
	SRCE_SYST_C,
	EFFT_D,
	EXPY_D,
	RUN_STRM 
FROM {{ ref('OutApptPdctCpgnDs') }}