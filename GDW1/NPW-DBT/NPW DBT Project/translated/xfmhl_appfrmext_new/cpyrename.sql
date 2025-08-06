{{ config(materialized='view', tags=['XfmHL_APPFrmExt_NEW']) }}

WITH CpyRename AS (
	SELECT
		HL_APP_ID,
		ORIG_ETL_D,
		HL_APP_PROD_ID,
		FHB_FLAG,
		SETTLEMENT_DATE,
		FOREIGN_INCOME_FLAG,
		FI_CURRENCY_CODE
	FROM {{ ref('Join_324') }}
)

SELECT * FROM CpyRename