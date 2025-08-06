{{ config(materialized='view', tags=['XfmCPL_APP_PROD_Appt_RelExt']) }}

WITH 
_cba__app_hlt_sit_dataset_cse__clp__bus__appt__rel__premap__20080310 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_sit_dataset_cse__clp__bus__appt__rel__premap__20080310")  }})
SrcCCAppProdBalXferPremapDS AS (
	SELECT LOAN_SUBTYPE_CODE,
		APPT_I,
		REL_TYPE_C,
		RELD_APPT_I,
		ORIG_ETL_D
	FROM _cba__app_hlt_sit_dataset_cse__clp__bus__appt__rel__premap__20080310
)

SELECT * FROM SrcCCAppProdBalXferPremapDS