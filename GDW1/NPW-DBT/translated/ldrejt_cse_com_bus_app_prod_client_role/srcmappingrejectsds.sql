{{ config(materialized='view', tags=['LdREJT_CSE_COM_BUS_APP_PROD_CLIENT_ROLE']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__com__bus__app__prod__client__role__mapping__rejects AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__com__bus__app__prod__client__role__mapping__rejects")  }})
SrcMappingRejectsDS AS (
	SELECT APP_PROD_CLIENT_ROLE_ID,
		ROLE_CAT_ID,
		CIF_CODE,
		APP_PROD_ID,
		SUBTYPE_CODE,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM _cba__app_csel4_csel4dev_dataset_cse__com__bus__app__prod__client__role__mapping__rejects
)

SELECT * FROM SrcMappingRejectsDS