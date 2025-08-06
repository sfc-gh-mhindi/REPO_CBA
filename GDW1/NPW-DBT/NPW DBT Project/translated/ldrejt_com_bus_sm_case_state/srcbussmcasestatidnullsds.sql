{{ config(materialized='view', tags=['LdREJT_COM_BUS_SM_CASE_STATE']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__com__bus__sm__case__state__smcaseid__nulls__rejects AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__com__bus__sm__case__state__smcaseid__nulls__rejects")  }})
SrcBusSmCaseStatIdNullsDS AS (
	SELECT SM_CASE_STATE_ID,
		SM_CASE_ID,
		SM_STATE_CAT_ID,
		START_DATE,
		END_DATE,
		CREATED_BY_STAFF_NUMBER,
		STATE_CAUSED_BY_ACTION_ID,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM _cba__app_csel4_csel4dev_dataset_cse__com__bus__sm__case__state__smcaseid__nulls__rejects
)

SELECT * FROM SrcBusSmCaseStatIdNullsDS