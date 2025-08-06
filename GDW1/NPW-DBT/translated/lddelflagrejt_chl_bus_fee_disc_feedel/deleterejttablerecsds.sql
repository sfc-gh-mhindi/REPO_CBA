{{ config(materialized='view', tags=['LdDelFlagREJT_CHL_BUS_FEE_DISC_FEEDel']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__chl__bus__fee__disc__fee__deleterejttablerecs AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__chl__bus__fee__disc__fee__deleterejttablerecs")  }})
DeleteRejtTableRecsDS AS (
	SELECT HL_FEE_ID
	FROM _cba__app_csel4_csel4dev_dataset_cse__chl__bus__fee__disc__fee__deleterejttablerecs
)

SELECT * FROM DeleteRejtTableRecsDS