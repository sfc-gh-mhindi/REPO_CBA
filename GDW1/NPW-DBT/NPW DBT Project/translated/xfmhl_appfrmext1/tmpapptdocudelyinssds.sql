{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__appt__docu__dely__inss', incremental_strategy='insert_overwrite', tags=['XfmHL_APPFrmExt1']) }}

SELECT
	APPT_I,
	DOCU_DELY_RECV_C,
	RUN_STRM 
FROM {{ ref('RdHllApptIdToInss') }}