{{ config(materialized='view', tags=['LdDelFlagREJT_CHL_BUS_FEAT_ATTRDel']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_hl__feature__attr__20061016 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_hl__feature__attr__20061016")  }})
HL_FEATURE_ATTR AS (
	SELECT DELETED_TABLE_NAME,
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
	FROM _cba__app_csel4_csel4dev_dataset_hl__feature__attr__20061016
)

SELECT * FROM HL_FEATURE_ATTR