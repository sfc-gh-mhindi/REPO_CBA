{{ config(materialized='view', tags=['LdDelFlagREJT_PL_FEE_PL_MARGINIns']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__fee__margin__insertrejttablerecs AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__fee__margin__insertrejttablerecs")  }})
InsertRejtTableRecsDS AS (
	SELECT PL_FEE_ID,
		PL_APP_PROD_ID,
		PL_FEE_PL_APP_PROD_ID,
		PL_MARGIN_PL_MARGIN_ID,
		PL_MARGIN_PL_FEE_ID,
		PL_MARGIN_MARGIN_REASON_CAT_ID,
		PL_FEE_FOUND_FLAG,
		PL_MARGIN_FOUND_FLAG,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM _cba__app_csel4_csel4dev_dataset_cse__cpl__bus__fee__margin__insertrejttablerecs
)

SELECT * FROM InsertRejtTableRecsDS