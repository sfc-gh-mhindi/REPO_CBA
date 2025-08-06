{{ config(materialized='view', tags=['MergeAppProdCclAppProdPlAppProd']) }}

WITH 
_cba__app_csel4_sit_inprocess_cse__com__bus__app__prod__cse__com__bus__app__prod__ccl__pl__app__prod__20120423 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_sit_inprocess_cse__com__bus__app__prod__cse__com__bus__app__prod__ccl__pl__app__prod__20120423")  }})
SrcComAppProdSeq AS (
	SELECT COM_RECORD_TYPE,
		COM_MOD_TIMESTAMP,
		COM_APP_PROD_ID,
		COM_SUBTYPE_CODE,
		COM_APP_ID,
		COM_PRODUCT_TYPE_ID,
		COM_SM_CASE_ID,
		COM_DUMMY
	FROM _cba__app_csel4_sit_inprocess_cse__com__bus__app__prod__cse__com__bus__app__prod__ccl__pl__app__prod__20120423
)

SELECT * FROM SrcComAppProdSeq