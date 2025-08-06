{{ config(materialized='view', tags=['XfmCCL_HL_APPFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__ccl__bus__hl__app__premap AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__ccl__bus__hl__app__premap")  }})
SrcApptRelPremapDS AS (
	SELECT CCL_HL_APP_ID,
		CCL_APP_ID,
		HL_APP_ID,
		LMI_AMT,
		HL_PACKAGE_CAT_ID,
		ORIG_ETL_D
	FROM _cba__app_csel4_csel4dev_dataset_cse__ccl__bus__hl__app__premap
)

SELECT * FROM SrcApptRelPremapDS