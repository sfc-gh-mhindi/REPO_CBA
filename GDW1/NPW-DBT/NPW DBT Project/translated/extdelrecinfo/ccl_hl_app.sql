{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_ccl__hl__app__20061016', incremental_strategy='insert_overwrite', tags=['ExtDelRecInfo']) }}

SELECT
	DELETED_RECORDS_INFO_ID,
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
FROM {{ ref('SwhDelRecInfo') }}