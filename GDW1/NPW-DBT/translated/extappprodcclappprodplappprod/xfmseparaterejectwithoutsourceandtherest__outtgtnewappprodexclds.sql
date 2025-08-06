{{ config(materialized='view', tags=['ExtAppProdCclAppProdPlAppProd']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__outTgtNewAppProdExclDS AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((OutLookupNotFound.APP_PROD_ID)) THEN (OutLookupNotFound.APP_PROD_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('LuAppProdExcl') }}.APP_PROD_ID IS NOT NULL, {{ ref('LuAppProdExcl') }}.APP_PROD_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		-- *SRC*: \(20)If (Trim(( IF IsNotNull((OutLookupNotFound.COM_SUBTYPE_CODE)) THEN (OutLookupNotFound.COM_SUBTYPE_CODE) ELSE "")) = 'CCL' and (trim(( IF IsNotNull((OutLookupNotFound.COM_PRODUCT_TYPE_ID)) THEN (OutLookupNotFound.COM_PRODUCT_TYPE_ID) ELSE "")) = '720999')) then 'Y' else 'N',
		IFF(TRIM(IFF({{ ref('LuAppProdExcl') }}.COM_SUBTYPE_CODE IS NOT NULL, {{ ref('LuAppProdExcl') }}.COM_SUBTYPE_CODE, '')) = 'CCL' AND TRIM(IFF({{ ref('LuAppProdExcl') }}.COM_PRODUCT_TYPE_ID IS NOT NULL, {{ ref('LuAppProdExcl') }}.COM_PRODUCT_TYPE_ID, '')) = '720999', 'Y', 'N') AS IgnoreF,
		APP_PROD_ID,
		'Y' AS DUMMY_PDCT_F,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS CRAT_D
	FROM {{ ref('LuAppProdExcl') }}
	WHERE IgnoreF = 'Y'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__outTgtNewAppProdExclDS