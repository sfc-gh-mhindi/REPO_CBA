{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_tmp__cse__prd__bus__prf__brk__data__unid__paty__gnrc', incremental_strategy='insert_overwrite', tags=['XfmBusPrfBrkDataFrmExt']) }}

SELECT
	UNID_PATY_I,
	RUN_STRM,
	SRCE_SYST_PATY_I 
FROM {{ ref('IgnrNulls__frmTrsfrmr') }}