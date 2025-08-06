{{ config(materialized='view', tags=['XfmComBusSmCaseFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__com__bus__sm__case__premap AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__com__bus__sm__case__premap")  }})
SrcPremapDS AS (
	SELECT SM_CASE_ID,
		CREATED_TIMESTAMP,
		WIM_PROCESS_ID,
		ORIG_ETL_D
	FROM _cba__app_csel4_csel4dev_dataset_cse__com__bus__sm__case__premap
)

SELECT * FROM SrcPremapDS