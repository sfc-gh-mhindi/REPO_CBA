{{ config(materialized='view', tags=['REJT_COI_BUS_CLNT_UNDTAK']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__coi__bus__clnt__undtak__plappprod__nulls__rejects AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__coi__bus__clnt__undtak__plappprod__nulls__rejects")  }})
SrcIdNullsDS AS (
	SELECT FA_CLIENT_UNDERTAKING_ID,
		FA_UNDERTAKING_ID,
		COIN_ENTITY_ID,
		CLIENT_CORRELATION_ID,
		FA_ENTITY_CAT_ID,
		FA_CHILD_STATUS_CAT_ID,
		CLIENT_RELATIONSHIP_TYPE_ID,
		CLIENT_POSITION,
		IS_PRIMARY_FLAG,
		CIF_CODE,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM _cba__app_csel4_csel4dev_dataset_cse__coi__bus__clnt__undtak__plappprod__nulls__rejects
)

SELECT * FROM SrcIdNullsDS