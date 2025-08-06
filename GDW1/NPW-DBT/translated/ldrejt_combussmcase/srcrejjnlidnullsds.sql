{{ config(materialized='view', tags=['LdREJT_ComBusSmCase']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__com__bus__sm__case__idnull__rejects AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__com__bus__sm__case__idnull__rejects")  }})
SrcRejJnlIdNullsDS AS (
	SELECT SM_CASE_ID,
		CREATED_TIMESTAMP,
		WIM_PROCESS_ID,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM _cba__app_csel4_csel4dev_dataset_cse__com__bus__sm__case__idnull__rejects
)

SELECT * FROM SrcRejJnlIdNullsDS