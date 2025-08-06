{{ config(materialized='incremental', alias='_cba__app_hlt_dev_dataset_cse__chl__bus__tu__app__cond__mapping__rejects__aqc', incremental_strategy='insert_overwrite', tags=['XfmTu_App_Cnd_XFERFrmExt']) }}

SELECT
	SBTY_CODE,
	HL_APP_PROD_ID,
	TU_APP_CONDITION_ID,
	COND_APPT_CAT_ID,
	CONDITION_MET_DATE,
	ETL_D,
	ORIG_ETL_D,
	EROR_C 
FROM {{ ref('OutRejApptQfy__rej_qlf') }}