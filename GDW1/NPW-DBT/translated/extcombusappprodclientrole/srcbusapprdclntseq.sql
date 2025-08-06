{{ config(materialized='view', tags=['ExtComBusAppProdClientRole']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__com__bus__app__prod__client__role__cse__com__bus__app__prod__client__role__20060616 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__com__bus__app__prod__client__role__cse__com__bus__app__prod__client__role__20060616")  }})
SrcBusApPrdClntSeq AS (
	SELECT RECORD_TYPE,
		MOD_TIMESTAMP,
		APP_PROD_CLIENT_ROLE_ID,
		ROLE_CAT_ID,
		CIF_CODE,
		APP_PROD_ID,
		SUBTYPE_CODE,
		DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_cse__com__bus__app__prod__client__role__cse__com__bus__app__prod__client__role__20060616
)

SELECT * FROM SrcBusApPrdClntSeq