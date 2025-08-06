{{ config(materialized='view', tags=['ExtBUS_HLM_APP']) }}

WITH XfmNotNullCheck AS (
	SELECT
		-- *SRC*: \(20)IF IsNull(Totrns.CRIS_PRODUCT_ID) THEN "N" ELSE  IF Trim(Totrns.CRIS_PRODUCT_ID) = '' THEN "N" ELSE "Y",
		IFF({{ ref('CpyPLAppProdSeq') }}.CRIS_PRODUCT_ID IS NULL, 'N', IFF(TRIM({{ ref('CpyPLAppProdSeq') }}.CRIS_PRODUCT_ID) = '', 'N', 'Y')) AS svValidRecord1,
		-- *SRC*: \(20)IF IsNull(Totrns.ACCOUNT_NUMBER) THEN "N" ELSE  IF Trim(Totrns.ACCOUNT_NUMBER) = '' THEN "N" ELSE "Y",
		IFF({{ ref('CpyPLAppProdSeq') }}.ACCOUNT_NUMBER IS NULL, 'N', IFF(TRIM({{ ref('CpyPLAppProdSeq') }}.ACCOUNT_NUMBER) = '', 'N', 'Y')) AS svValidRecord2,
		-- *SRC*: \(20)IF IsNull(Totrns.HL_APP_PROD_ID) THEN "N" ELSE  IF Trim(Totrns.HL_APP_PROD_ID) = '' THEN "N" ELSE "Y",
		IFF({{ ref('CpyPLAppProdSeq') }}.HL_APP_PROD_ID IS NULL, 'N', IFF(TRIM({{ ref('CpyPLAppProdSeq') }}.HL_APP_PROD_ID) = '', 'N', 'Y')) AS svValidRecord3,
		APP_ID,
		HLM_ACCOUNT_ID,
		ACCOUNT_NUMBER,
		CRIS_PRODUCT_ID,
		HLM_APP_TYPE_CAT_ID,
		{{ ref('CpyPLAppProdSeq') }}.DISCHARGE_REASON_ID AS DCHG_REAS_ID,
		HL_APP_PROD_ID
	FROM {{ ref('CpyPLAppProdSeq') }}
	WHERE svValidRecord1 = 'Y' AND svValidRecord2 = 'Y' AND svValidRecord3 = 'Y'
)

SELECT * FROM XfmNotNullCheck