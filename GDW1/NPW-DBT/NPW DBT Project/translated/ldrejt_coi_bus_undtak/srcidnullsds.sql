{{ config(materialized='view', tags=['LdREJT_COI_BUS_UNDTAK']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__coi__bus__undtak__nulls__rejects AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__coi__bus__undtak__nulls__rejects")  }})
SrcIdNullsDS AS (
	SELECT FA_UNDERTAKING_ID,
		PLANNING_GROUP_NAME,
		COIN_ADVICE_GROUP_ID,
		ADVICE_GROUP_CORRELATION_ID,
		CREATED_DATE,
		CREATED_BY_STAFF_NUMBER,
		SM_CASE_ID,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM _cba__app_csel4_csel4dev_dataset_cse__coi__bus__undtak__nulls__rejects
)

SELECT * FROM SrcIdNullsDS