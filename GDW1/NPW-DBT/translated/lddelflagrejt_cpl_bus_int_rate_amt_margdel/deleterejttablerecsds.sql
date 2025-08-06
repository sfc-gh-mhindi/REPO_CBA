{{ config(materialized='view', tags=['LdDelFlagREJT_CPL_BUS_INT_RATE_AMT_MARGDel']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__int__rate__amt__margin__deleterejttablerecs AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__int__rate__amt__margin__deleterejttablerecs")  }})
DeleteRejtTableRecsDS AS (
	SELECT PL_INT_RATE_ID
	FROM _cba__app_csel4_csel4dev_dataset_cse__cpl__bus__int__rate__amt__margin__deleterejttablerecs
)

SELECT * FROM DeleteRejtTableRecsDS