{{ config(materialized='view', tags=['LdREJT_CSE_CPL_BUS_APP']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__app__plapp__nulls__rejects AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__app__plapp__nulls__rejects")  }})
RejCseCplBusDS AS (
	SELECT PL_APP_ID,
		NOMINATED_BRANCH_ID,
		PL_PACKAGE_CAT_ID,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM _cba__app_csel4_csel4dev_dataset_cse__cpl__bus__app__plapp__nulls__rejects
)

SELECT * FROM RejCseCplBusDS