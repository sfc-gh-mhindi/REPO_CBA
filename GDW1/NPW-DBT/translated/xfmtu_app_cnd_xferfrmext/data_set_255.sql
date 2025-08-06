{{ config(materialized='incremental', alias='_cba__app_hlt_dev_dataset_cse__chl__bus__tu__app__cond__appt__pdct__cond', incremental_strategy='insert_overwrite', tags=['XfmTu_App_Cnd_XFERFrmExt']) }}

SELECT
	APPT_PDCT_I,
	COND_C,
	APPT_PDCT_COND_MEET_D 
FROM {{ ref('Trm_O__valid_Appt_Pdct_cond') }}