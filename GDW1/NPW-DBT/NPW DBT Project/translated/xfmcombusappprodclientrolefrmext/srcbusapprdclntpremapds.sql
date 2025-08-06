{{ config(materialized='view', tags=['XfmComBusAppProdClientRoleFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__com__bus__app__prod__client__role__premap AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__com__bus__app__prod__client__role__premap")  }})
SrcBusApPrdClntPremapDS AS (
	SELECT APP_PROD_CLIENT_ROLE_ID,
		ROLE_CAT_ID,
		CIF_CODE,
		APP_PROD_ID,
		SUBTYPE_CODE,
		ORIG_ETL_D
	FROM _cba__app_csel4_csel4dev_dataset_cse__com__bus__app__prod__client__role__premap
)

SELECT * FROM SrcBusApPrdClntPremapDS