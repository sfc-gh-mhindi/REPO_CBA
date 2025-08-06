{{ config(materialized='view', tags=['LdTMP_APPT_PRMO_TRAKFrmXfm']) }}

WITH 
____var__dbt__pdataset_____ccl__app__prod__trak______var__dbt__prun__strm__pros__d____ AS (
	SELECT
	*
	FROM {{ source("","____var__dbt__pdataset_____ccl__app__prod__trak______var__dbt__prun__strm__pros__d____")  }})
ds_CclAppProdTrak AS (
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
	FROM ____var__dbt__pdataset_____ccl__app__prod__trak______var__dbt__prun__strm__pros__d____
)

SELECT * FROM ds_CclAppProdTrak