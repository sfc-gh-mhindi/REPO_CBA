{{ config(materialized='view', tags=['LdREJT_ChlBusFeeDiscFee']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__chl__bus__fee__disc__fee__mapping__rejects AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__chl__bus__fee__disc__fee__mapping__rejects")  }})
SrcMappingDS AS (
	SELECT HL_FEE_ID,
		BF_HL_FEE_ID,
		BF_HL_APP_PROD_ID,
		BF_XML_CODE,
		BF_DISPLAY_NAME,
		BF_CATEGORY,
		BF_UNIT_AMOUNT,
		BF_TOTAL_AMOUNT,
		BFD_HL_FEE_DISCOUNT_ID,
		BFD_HL_FEE_ID,
		BFD_DISCOUNT_REASON,
		BFD_DISCOUNT_CODE,
		BFD_DISCOUNT_AMT,
		BFD_DISCOUNT_TERM,
		BFD_HL_FEE_DISCOUNT_CAT_ID,
		BF_FOUND_FLAG,
		BFD_FOUND_FLAG,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM _cba__app_csel4_csel4dev_dataset_cse__chl__bus__fee__disc__fee__mapping__rejects
)

SELECT * FROM SrcMappingDS