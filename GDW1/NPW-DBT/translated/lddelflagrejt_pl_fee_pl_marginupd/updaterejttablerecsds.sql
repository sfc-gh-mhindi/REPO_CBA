{{ config(materialized='view', tags=['LdDelFlagREJT_PL_FEE_PL_MARGINUpd']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__fee__margin__updaterejttablerecs AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__fee__margin__updaterejttablerecs")  }})
UpdateRejtTableRecsDS AS (
	SELECT PL_FEE_ID,
		PL_MARGIN_MARGIN_AMT,
		PL_MARGIN_MARGIN_REASON_CAT_ID,
		PL_MARGIN_FOUND_FLAG,
		ETL_D
	FROM _cba__app_csel4_csel4dev_dataset_cse__cpl__bus__fee__margin__updaterejttablerecs
)

SELECT * FROM UpdateRejtTableRecsDS