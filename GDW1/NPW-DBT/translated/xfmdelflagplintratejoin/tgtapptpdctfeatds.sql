{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_appt__pdct__feat__20061016', incremental_strategy='insert_overwrite', tags=['XfmDelFlagPlIntRateJoin']) }}

SELECT
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
FROM {{ ref('XfmPlIntRate__ExpireTargetTableRecs') }}