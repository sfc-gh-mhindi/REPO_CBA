{{ config(materialized='view', tags=['LdDelFlagREJT_CHL_BUS_FEE_DISC_FEEUpd']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__chl__bus__fee__disc__fee__updaterejttablerecs AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__chl__bus__fee__disc__fee__updaterejttablerecs")  }})
UpdateRejtTableRecsDS AS (
	SELECT HL_FEE_ID,
		BFD_DISCOUNT_REASON,
		BFD_DISCOUNT_CODE,
		BFD_DISCOUNT_AMT,
		BFD_DISCOUNT_TERM,
		BFD_HL_FEE_DISCOUNT_CAT_ID,
		BFD_FOUND_FLAG,
		ETL_D
	FROM _cba__app_csel4_csel4dev_dataset_cse__chl__bus__fee__disc__fee__updaterejttablerecs
)

SELECT * FROM UpdateRejtTableRecsDS