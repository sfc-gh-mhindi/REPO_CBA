{{ config(materialized='view', tags=['XfmPL_APPFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__app__premap AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__app__premap")  }})
SrcPlAppPremapDS AS (
	SELECT PL_APP_ID,
		NOMINATED_BRANCH_ID,
		PL_PACKAGE_CAT_ID,
		ORIG_ETL_D
	FROM _cba__app_csel4_csel4dev_dataset_cse__cpl__bus__app__premap
)

SELECT * FROM SrcPlAppPremapDS