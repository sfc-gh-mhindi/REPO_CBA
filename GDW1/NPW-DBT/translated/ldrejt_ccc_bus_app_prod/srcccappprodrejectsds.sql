{{ config(materialized='view', tags=['LdREJT_CCC_BUS_APP_PROD']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__ccc__bus__app__prod__mapping__rejects AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__ccc__bus__app__prod__mapping__rejects")  }})
SrcCCAppProdRejectsDS AS (
	SELECT CC_APP_PROD_ID,
		REQUESTED_LIMIT_AMT,
		CC_INTEREST_OPT_CAT_ID,
		CBA_HOMELOAN_NO,
		PRE_APPRV_AMOUNT,
		ETL_D,
		ORIG_ETL_D,
		READ_COSTS_AND_RISKS_FLAG,
		ACCEPTS_COSTS_AND_RISKS_DATE,
		EROR_C
	FROM _cba__app_csel4_csel4dev_dataset_cse__ccc__bus__app__prod__mapping__rejects
)

SELECT * FROM SrcCCAppProdRejectsDS