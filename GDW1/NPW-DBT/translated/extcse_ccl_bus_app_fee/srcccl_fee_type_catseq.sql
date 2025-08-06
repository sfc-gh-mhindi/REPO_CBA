{{ config(materialized='view', tags=['ExtCSE_CCL_BUS_APP_FEE']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__ccl__ref__fee__type__cat__cse__ccl__bus__app__fee__20060616 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__ccl__ref__fee__type__cat__cse__ccl__bus__app__fee__20060616")  }})
SrcCCL_FEE_TYPE_CATSeq AS (
	SELECT RECORD_TYPE,
		CCL_FEE_TYPE_CAT_ID,
		CCL_FEE_TYPE_CAT_FEE_TYPE_DESC,
		CCL_FEE_TYPE_CAT_DEFAULT_AMT,
		CCL_FEE_TYPE_CAT_DEFAULT_FEE_PCT,
		DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_cse__ccl__ref__fee__type__cat__cse__ccl__bus__app__fee__20060616
)

SELECT * FROM SrcCCL_FEE_TYPE_CATSeq