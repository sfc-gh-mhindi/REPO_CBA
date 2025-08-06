{{ config(materialized='view', tags=['XfmHL_APP_PROD_PURPOSEFrmExt']) }}

WITH CpyRename AS (
	SELECT
		HL_APP_PROD_PURPOSE_ID,
		HL_APP_PROD_ID,
		HL_LOAN_PURPOSE_CAT_ID,
		AMOUNT,
		MAIN_PURPOSE,
		ORIG_ETL_D
	FROM {{ ref('SrcHlAppProdPurpPremapDS') }}
)

SELECT * FROM CpyRename