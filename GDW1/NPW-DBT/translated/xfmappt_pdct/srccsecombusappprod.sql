{{ config(materialized='view', tags=['XfmAppt_Pdct']) }}

WITH 
_cba__app_csel4_dev_inprocess_cse__com__bus__app__prod__cse__com__bus__app__prod__ccl__pl__app__prod__20101030 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_inprocess_cse__com__bus__app__prod__cse__com__bus__app__prod__ccl__pl__app__prod__20101030")  }})
SrcCseComBusAppProd AS (
	SELECT Record_Type,
		MOD_TIMESTAMP,
		APP_PROD_ID,
		SUBTYPE_CODE,
		APP_ID,
		PDCT_N,
		SM_CASE_ID
	FROM _cba__app_csel4_dev_inprocess_cse__com__bus__app__prod__cse__com__bus__app__prod__ccl__pl__app__prod__20101030
)

SELECT * FROM SrcCseComBusAppProd