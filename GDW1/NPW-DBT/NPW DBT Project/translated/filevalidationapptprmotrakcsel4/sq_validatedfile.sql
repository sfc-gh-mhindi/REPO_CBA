{{ config(materialized='incremental', alias='_cba__app_csel4_dev_inprocess_rdh__test', incremental_strategy='insert_overwrite', tags=['FileValidationApptPrmoTrakCSEL4']) }}

SELECT
	RECD_TYPE,
	REST_OF_RECD 
FROM {{ ref('xf_Validate__Ln_Write_To_Vald') }}