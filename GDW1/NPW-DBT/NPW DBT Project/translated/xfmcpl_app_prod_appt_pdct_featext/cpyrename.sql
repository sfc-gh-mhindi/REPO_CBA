{{ config(materialized='view', tags=['XfmCPL_APP_PROD_Appt_Pdct_FeatExt']) }}

WITH CpyRename AS (
	SELECT
		APP_PROD_ID,
		{{ ref('SrcCCAppProdBalXferPremapDS') }}.COM_SUBTYPE_CODE AS SBTY_CODE,
		CAMPAIGN_CAT_ID,
		COM_APP_ID,
		ORIG_ETL_D
	FROM {{ ref('SrcCCAppProdBalXferPremapDS') }}
)

SELECT * FROM CpyRename