{{ config(materialized='view', tags=['XfmTu_App_Cnd_XFERFrmExt']) }}

WITH 
_cba__app_hlt_dev_dataset_cse__chl__bus__tu__app__cond__premap AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_dev_dataset_cse__chl__bus__tu__app__cond__premap")  }})
SrcApptPdctFnddInssPremapDS AS (
	SELECT SUBTYPE_CODE,
		HL_APP_PROD_ID,
		TU_APP_CONDITION_ID,
		TU_APP_CONDITION_CAT_ID,
		CONDITION_MET_DATE,
		ORIG_ETL_D
	FROM _cba__app_hlt_dev_dataset_cse__chl__bus__tu__app__cond__premap
)

SELECT * FROM SrcApptPdctFnddInssPremapDS