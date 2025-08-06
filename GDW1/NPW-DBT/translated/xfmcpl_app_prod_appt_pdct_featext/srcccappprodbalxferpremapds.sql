{{ config(materialized='view', tags=['XfmCPL_APP_PROD_Appt_Pdct_FeatExt']) }}

WITH 
_cba__app_csel4_csel4__prd_dataset_cse__clp__bus__appt__pdct__feat__premap AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4__prd_dataset_cse__clp__bus__appt__pdct__feat__premap")  }})
SrcCCAppProdBalXferPremapDS AS (
	SELECT APP_PROD_ID,
		COM_SUBTYPE_CODE,
		CAMPAIGN_CAT_ID,
		COM_APP_ID,
		ORIG_ETL_D
	FROM _cba__app_csel4_csel4__prd_dataset_cse__clp__bus__appt__pdct__feat__premap
)

SELECT * FROM SrcCCAppProdBalXferPremapDS