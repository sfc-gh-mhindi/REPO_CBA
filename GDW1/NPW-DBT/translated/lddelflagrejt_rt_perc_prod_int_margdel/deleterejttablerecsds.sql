{{ config(materialized='view', tags=['LdDelFlagREJT_RT_PERC_PROD_INT_MARGDel']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__chl__bus__int__rt__prc__prd__int__mrg__deleterejttablerecs AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__chl__bus__int__rt__prc__prd__int__mrg__deleterejttablerecs")  }})
DeleteRejtTableRecsDS AS (
	SELECT HL_INT_RATE_ID
	FROM _cba__app_csel4_csel4dev_dataset_cse__chl__bus__int__rt__prc__prd__int__mrg__deleterejttablerecs
)

SELECT * FROM DeleteRejtTableRecsDS