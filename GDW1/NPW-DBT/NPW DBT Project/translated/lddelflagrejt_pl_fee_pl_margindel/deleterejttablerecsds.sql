{{ config(materialized='view', tags=['LdDelFlagREJT_PL_FEE_PL_MARGINDel']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__fee__margin__deleterejttablerecs AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__fee__margin__deleterejttablerecs")  }})
DeleteRejtTableRecsDS AS (
	SELECT PL_FEE_ID
	FROM _cba__app_csel4_csel4dev_dataset_cse__cpl__bus__fee__margin__deleterejttablerecs
)

SELECT * FROM DeleteRejtTableRecsDS