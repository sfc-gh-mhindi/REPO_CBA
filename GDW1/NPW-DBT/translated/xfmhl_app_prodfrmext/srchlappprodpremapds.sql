{{ config(materialized='view', tags=['XfmHL_APP_PRODFrmExt']) }}

WITH 
_cba__app_mme_dev_dataset_cse__chl__bus__app__prod__premap AS (
	SELECT
	*
	FROM {{ source("","_cba__app_mme_dev_dataset_cse__chl__bus__app__prod__premap")  }})
SrcHlAppProdPremapDS AS (
	SELECT HL_APP_PROD_ID,
		PARENT_HL_APP_PROD_ID,
		HL_REPAYMENT_PERIOD_CAT_ID,
		AMOUNT,
		LOAN_TERM_MONTHS,
		ACCOUNT_NUMBER,
		TOTAL_LOAN_AMOUNT,
		HLS_FLAG,
		GDW_UPDATED_LDP_PAID_ON_AMOUNT,
		ORIG_ETL_D
	FROM _cba__app_mme_dev_dataset_cse__chl__bus__app__prod__premap
)

SELECT * FROM SrcHlAppProdPremapDS