{{ config(materialized='view', tags=['FileValidationApptPrmoTrakCSEL4']) }}

WITH 
_cba__app_csel4_dev_inbound_commcdata_rdh__test AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_inbound_commcdata_rdh__test")  }})
sq_SrceFileData AS (
	SELECT RECD_TYPE,
		REST_OF_RECD,
		ROW_NUMBER
	FROM _cba__app_csel4_dev_inbound_commcdata_rdh__test
)

SELECT * FROM sq_SrceFileData