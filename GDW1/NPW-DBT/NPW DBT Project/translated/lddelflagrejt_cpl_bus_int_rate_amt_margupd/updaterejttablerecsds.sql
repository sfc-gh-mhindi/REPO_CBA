{{ config(materialized='view', tags=['LdDelFlagREJT_CPL_BUS_INT_RATE_AMT_MARGUpd']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__int__rate__amt__margin__updaterejttablerecs AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__int__rate__amt__margin__updaterejttablerecs")  }})
UpdateRejtTableRecsDS AS (
	SELECT PL_INT_RATE_ID,
		PL_MARGIN_MARGIN_AMT,
		PL_MARGIN_MARGIN_RESN_CAT_ID,
		PL_MARGIN_FOUND_FLAG,
		ETL_D
	FROM _cba__app_csel4_csel4dev_dataset_cse__cpl__bus__int__rate__amt__margin__updaterejttablerecs
)

SELECT * FROM UpdateRejtTableRecsDS