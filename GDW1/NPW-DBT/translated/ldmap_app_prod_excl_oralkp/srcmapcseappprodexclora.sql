{{ config(materialized='view', tags=['LdMAP_APP_PROD_EXCL_ORALkp']) }}

WITH 
,
SrcMapCseAppProdExclOra AS (SELECT
		APP_PROD_ID,
		DUMMY_PDCT_F
	FROM )


SELECT * FROM SrcMapCseAppProdExclOra