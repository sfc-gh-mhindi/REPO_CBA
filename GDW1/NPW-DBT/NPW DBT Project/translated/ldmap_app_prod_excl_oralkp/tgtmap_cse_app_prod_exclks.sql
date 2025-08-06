{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__app__prod__excl__app__prod__id', incremental_strategy='insert_overwrite', tags=['LdMAP_APP_PROD_EXCL_ORALkp']) }}

SELECT
	APP_PROD_ID,
	DUMMY_PDCT_F 
FROM {{ ref('XfmLoadMapCseAppProdExcl') }}