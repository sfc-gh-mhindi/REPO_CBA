{{ config(materialized='view', tags=['ExtAppProdComAppProdClp']) }}

WITH XfmCheckNulls__OutCheckNullsSorted AS (
	SELECT
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((XfmCheckNulls.CAMPAIGN_CAT_ID)) THEN (XfmCheckNulls.CAMPAIGN_CAT_ID) ELSE ""))) = 0 Then 'REJ792' else  if Len(Trim(( IF IsNotNull((XfmCheckNulls.APP_PROD_ID)) THEN (XfmCheckNulls.APP_PROD_ID) ELSE ""))) = 0 Then 'REJ791' Else '',
		IFF(LEN(TRIM(IFF({{ ref('Cpy_NoOp') }}.CAMPAIGN_CAT_ID IS NOT NULL, {{ ref('Cpy_NoOp') }}.CAMPAIGN_CAT_ID, ''))) = 0, 'REJ792', IFF(LEN(TRIM(IFF({{ ref('Cpy_NoOp') }}.APP_PROD_ID IS NOT NULL, {{ ref('Cpy_NoOp') }}.APP_PROD_ID, ''))) = 0, 'REJ791', '')) AS ErrorCode,
		APP_PROD_ID,
		COM_SUBTYPE_CODE,
		CAMPAIGN_CAT_ID,
		COM_APP_ID
	FROM {{ ref('Cpy_NoOp') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) <> 'REJ'
)

SELECT * FROM XfmCheckNulls__OutCheckNullsSorted