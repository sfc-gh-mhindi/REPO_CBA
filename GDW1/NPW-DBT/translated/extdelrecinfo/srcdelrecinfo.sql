{{ config(materialized='view', tags=['ExtDelRecInfo']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__com__bus__del__rec__info__cse__com__bus__del__rec__info__20061016 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__com__bus__del__rec__info__cse__com__bus__del__rec__info__20061016")  }})
SrcDelRecInfo AS (
	SELECT DELETED_RECORDS_INFO_ID,
		DELETED_TIMESTAMP,
		DELETED_TABLE_OWNER,
		DELETED_TABLE_NAME,
		DELETED_KEY_1,
		DELETED_KEY_1_VALUE,
		DELETED_KEY_2,
		DELETED_KEY_2_VALUE,
		DELETED_KEY_3,
		DELETED_KEY_3_VALUE,
		DELETED_KEY_4,
		DELETED_KEY_4_VALUE,
		DELETED_KEY_5,
		DELETED_KEY_5_VALUE
	FROM _cba__app_csel4_csel4dev_inprocess_cse__com__bus__del__rec__info__cse__com__bus__del__rec__info__20061016
)

SELECT * FROM SrcDelRecInfo