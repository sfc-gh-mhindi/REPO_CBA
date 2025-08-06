{{ config(materialized='view', tags=['XfmAppt_Pdct_Purp']) }}

WITH Transformer AS (
	SELECT
		-- *SRC*: \(20)If IsNull(FrmSrc.APP_PROD_ID) Then 'Y' Else  if Trim(( IF IsNotNull((FrmSrc.APP_PROD_ID)) THEN (FrmSrc.APP_PROD_ID) ELSE "")) = '' THEN 'Y' Else  if Trim(( IF IsNotNull((FrmSrc.APP_PROD_ID)) THEN (FrmSrc.APP_PROD_ID) ELSE "")) = 0 THEN 'Y' ELSE 'N',
		IFF({{ ref('CSE_CPO_BUS_APP_PROD') }}.APP_PROD_ID IS NULL, 'Y', IFF(TRIM(IFF({{ ref('CSE_CPO_BUS_APP_PROD') }}.APP_PROD_ID IS NOT NULL, {{ ref('CSE_CPO_BUS_APP_PROD') }}.APP_PROD_ID, '')) = '', 'Y', IFF(TRIM(IFF({{ ref('CSE_CPO_BUS_APP_PROD') }}.APP_PROD_ID IS NOT NULL, {{ ref('CSE_CPO_BUS_APP_PROD') }}.APP_PROD_ID, '')) = 0, 'Y', 'N'))) AS svIsNullAppProdId,
		-- *SRC*: \(20)If IsNull(FrmSrc.PL_PROD_PURP_CAT_ID) Then 'Y' Else  if Trim(FrmSrc.PL_PROD_PURP_CAT_ID) = '0' Then 'Y' Else  if Trim(( IF IsNotNull((FrmSrc.PL_PROD_PURP_CAT_ID)) THEN (FrmSrc.PL_PROD_PURP_CAT_ID) ELSE "")) = '' THEN 'Y' ELSE 'N',
		IFF({{ ref('CSE_CPO_BUS_APP_PROD') }}.PL_PROD_PURP_CAT_ID IS NULL, 'Y', IFF(TRIM({{ ref('CSE_CPO_BUS_APP_PROD') }}.PL_PROD_PURP_CAT_ID) = '0', 'Y', IFF(TRIM(IFF({{ ref('CSE_CPO_BUS_APP_PROD') }}.PL_PROD_PURP_CAT_ID IS NOT NULL, {{ ref('CSE_CPO_BUS_APP_PROD') }}.PL_PROD_PURP_CAT_ID, '')) = '', 'Y', 'N'))) AS svIsNullPlProdPurpCatId,
		APP_PROD_ID,
		PL_PROD_PURP_CAT_ID
	FROM {{ ref('CSE_CPO_BUS_APP_PROD') }}
	WHERE svIsNullAppProdId = 'N' AND svIsNullPlProdPurpCatId = 'N'
)

SELECT * FROM Transformer