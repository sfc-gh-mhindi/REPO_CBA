{{ config(materialized='view', tags=['LdMAP_APP_PROD_EXCL_ORALkp']) }}

WITH XfmLoadMapCseAppProdExcl AS (
	SELECT
		-- *SRC*: trim(InMapCseAppProdExclOra.APP_PROD_ID),
		TRIM({{ ref('SrcMapCseAppProdExclOra') }}.APP_PROD_ID) AS APP_PROD_ID,
		-- *SRC*: UpCase(InMapCseAppProdExclOra.DUMMY_PDCT_F),
		UPPER({{ ref('SrcMapCseAppProdExclOra') }}.DUMMY_PDCT_F) AS DUMMY_PDCT_F
	FROM {{ ref('SrcMapCseAppProdExclOra') }}
	WHERE 
)

SELECT * FROM XfmLoadMapCseAppProdExcl