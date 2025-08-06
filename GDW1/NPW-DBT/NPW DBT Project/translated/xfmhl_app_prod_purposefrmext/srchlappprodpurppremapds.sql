{{ config(materialized='view', tags=['XfmHL_APP_PROD_PURPOSEFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__chl__bus__app__prod__purp__premap AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__chl__bus__app__prod__purp__premap")  }})
SrcHlAppProdPurpPremapDS AS (
	SELECT HL_APP_PROD_PURPOSE_ID,
		HL_APP_PROD_ID,
		HL_LOAN_PURPOSE_CAT_ID,
		AMOUNT,
		MAIN_PURPOSE,
		ORIG_ETL_D
	FROM _cba__app_csel4_csel4dev_dataset_cse__chl__bus__app__prod__purp__premap
)

SELECT * FROM SrcHlAppProdPurpPremapDS