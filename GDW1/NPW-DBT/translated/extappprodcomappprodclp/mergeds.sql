{{ config(materialized='view', tags=['ExtAppProdComAppProdClp']) }}

WITH 
_cba__app_csel4_csel4__prd_dataset_cse__clp__bus__appt__pdct__feat__merge__20080406 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4__prd_dataset_cse__clp__bus__appt__pdct__feat__merge__20080406")  }})
MergeDS AS (
	SELECT APP_PROD_ID,
		COM_SUBTYPE_CODE,
		CAMPAIGN_CAT_ID,
		COM_APP_ID
	FROM _cba__app_csel4_csel4__prd_dataset_cse__clp__bus__appt__pdct__feat__merge__20080406
)

SELECT * FROM MergeDS