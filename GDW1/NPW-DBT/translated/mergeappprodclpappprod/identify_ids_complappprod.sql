{{ config(materialized='view', tags=['MergeAppProdClpAppProd']) }}

WITH Identify_IDs_ComPlAppProd AS (
	SELECT
		-- *SRC*: \(20)IF trim(Len(( IF IsNotNull((InClpAppProdSeq.CAMPAIGN_CAT_ID)) THEN (InClpAppProdSeq.CAMPAIGN_CAT_ID) ELSE ""))) = '0' Then 'N' Else 'Y',
		IFF(TRIM(LEN(IFF({{ ref('SrcClpAppProdSeq') }}.CAMPAIGN_CAT_ID IS NOT NULL, {{ ref('SrcClpAppProdSeq') }}.CAMPAIGN_CAT_ID, ''))) = '0', 'N', 'Y') AS LOAD,
		{{ ref('SrcClpAppProdSeq') }}.CLP_APP_PROD_ID AS APP_PROD_ID,
		'CLP' AS COM_SUBTYPE_CODE,
		CAMPAIGN_CAT_ID,
		'XYZ123' AS COM_APP_ID
	FROM {{ ref('SrcClpAppProdSeq') }}
	WHERE LOAD = 'Y'
)

SELECT * FROM Identify_IDs_ComPlAppProd