{{ config(materialized='view', tags=['LdCOMMONFnl']) }}

WITH XfmDelFlag__InAPPT_PDCT_PURP AS (
	SELECT
		-- *SRC*: \(20)If OutDelFlag.DELETED_TABLE_NAME = 'APP_PROD_CLIENT_ROLE' Then 'Y' Else 'N',
		IFF({{ ref('FnlDelFlag') }}.DELETED_TABLE_NAME = 'APP_PROD_CLIENT_ROLE', 'Y', 'N') AS svAppProdClntRole,
		-- *SRC*: \(20)If OutDelFlag.DELETED_TABLE_NAME = 'CCL_APP_PROD' Then 'Y' Else 'N',
		IFF({{ ref('FnlDelFlag') }}.DELETED_TABLE_NAME = 'CCL_APP_PROD', 'Y', 'N') AS svCclAppProd,
		-- *SRC*: \(20)If OutDelFlag.DELETED_TABLE_NAME = 'HL_APP_PROD' Then 'Y' Else 'N',
		IFF({{ ref('FnlDelFlag') }}.DELETED_TABLE_NAME = 'HL_APP_PROD', 'Y', 'N') AS svHlAppProd,
		-- *SRC*: \(20)If OutDelFlag.DELETED_TABLE_NAME = 'PL_APP_PROD' Then 'Y' Else 'N',
		IFF({{ ref('FnlDelFlag') }}.DELETED_TABLE_NAME = 'PL_APP_PROD', 'Y', 'N') AS svPlAppProd,
		-- *SRC*: \(20)If OutDelFlag.DELETED_TABLE_NAME = 'CC_APP_PROD' Then 'Y' Else 'N',
		IFF({{ ref('FnlDelFlag') }}.DELETED_TABLE_NAME = 'CC_APP_PROD', 'Y', 'N') AS svCcAppProd,
		-- *SRC*: \(20)If OutDelFlag.DELETED_TABLE_NAME = 'CCL_HL_APP' Then 'Y' Else 'N',
		IFF({{ ref('FnlDelFlag') }}.DELETED_TABLE_NAME = 'CCL_HL_APP', 'Y', 'N') AS svCclHlApp,
		-- *SRC*: \(20)If OutDelFlag.DELETED_TABLE_NAME = 'CCL_APP_LINK' Then 'Y' Else 'N',
		IFF({{ ref('FnlDelFlag') }}.DELETED_TABLE_NAME = 'CCL_APP_LINK', 'Y', 'N') AS svCclAppLink,
		-- *SRC*: \(20)If OutDelFlag.DELETED_TABLE_NAME = 'HL_APP_PROD_PURPOSE' Then 'Y' Else 'N',
		IFF({{ ref('FnlDelFlag') }}.DELETED_TABLE_NAME = 'HL_APP_PROD_PURPOSE', 'Y', 'N') AS svHlAppProdPurpose,
		-- *SRC*: \(20)If OutDelFlag.DELETED_TABLE_NAME = 'PL_APP_PROD_PURP' Then 'Y' Else 'N',
		IFF({{ ref('FnlDelFlag') }}.DELETED_TABLE_NAME = 'PL_APP_PROD_PURP', 'Y', 'N') AS svPlAppProdPurp,
		-- *SRC*: \(20)If OutDelFlag.DELETED_TABLE_NAME = 'HL_FEATURE_ATTR' Then 'Y' Else 'N',
		IFF({{ ref('FnlDelFlag') }}.DELETED_TABLE_NAME = 'HL_FEATURE_ATTR', 'Y', 'N') AS svHlFeatureAttr,
		-- *SRC*: \(20)If OutDelFlag.DELETED_TABLE_NAME = 'PL_FEE' Then 'Y' Else 'N',
		IFF({{ ref('FnlDelFlag') }}.DELETED_TABLE_NAME = 'PL_FEE', 'Y', 'N') AS svPlFee,
		-- *SRC*: \(20)If OutDelFlag.DELETED_TABLE_NAME = 'CCL_APP_FEE' Then 'Y' Else 'N',
		IFF({{ ref('FnlDelFlag') }}.DELETED_TABLE_NAME = 'CCL_APP_FEE', 'Y', 'N') AS svCclAppFee,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((OutDelFlag.DELETED_KEY_3_VALUE)) THEN (OutDelFlag.DELETED_KEY_3_VALUE) ELSE ""))) = 0) Then 'Y' Else 'N',
		IFF(LEN(TRIM(IFF({{ ref('FnlDelFlag') }}.DELETED_KEY_3_VALUE IS NOT NULL, {{ ref('FnlDelFlag') }}.DELETED_KEY_3_VALUE, ''))) = 0, 'Y', 'N') AS svIsDel3Null,
		DELETED_TABLE_NAME,
		DELETED_KEY_1,
		DELETED_KEY_1_VALUE,
		DELETED_KEY_2,
		DELETED_KEY_2_VALUE,
		DELETED_KEY_3,
		DELETED_KEY_3_VALUE,
		DELETED_KEY_4,
		DELETED_KEY_4_VALUE,
		DELETED_KEY_5,
		DELETED_KEY_5_VALUE
	FROM {{ ref('FnlDelFlag') }}
	WHERE svCclAppProd = 'Y' OR svHlAppProdPurpose = 'Y' OR svPlAppProdPurp = 'Y'
)

SELECT * FROM XfmDelFlag__InAPPT_PDCT_PURP