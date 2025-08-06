{{ config(materialized='view', tags=['LdDelFlagREJT_CPL_BUS_INT_RATE_AMT_MARGIns']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__int__rate__amt__margin__insertrejttablerecs AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__int__rate__amt__margin__insertrejttablerecs")  }})
InsertRejtTableRecsDS AS (
	SELECT PL_INT_RATE_ID,
		PL_MARGIN_PL_MARGIN_ID,
		PL_MARGIN_PL_INT_RATE_ID,
		PL_MARGIN_MARGIN_RESN_CAT_ID,
		PL_MARGIN_FOUND_FLAG,
		PL_INT_RATE_FOUND_FLAG,
		PL_INT_RATE_AMT_FOUND_FLAG,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM _cba__app_csel4_csel4dev_dataset_cse__cpl__bus__int__rate__amt__margin__insertrejttablerecs
)

SELECT * FROM InsertRejtTableRecsDS