{{ config(materialized='view', tags=['XfmAppt_Pdct_Paty']) }}

WITH Transformer AS (
	SELECT
		-- *SRC*: \(20)If IsNull(FrmSrc.APP_PROD_ID) Then 'Y' Else  if Trim(( IF IsNotNull((FrmSrc.APP_PROD_ID)) THEN (FrmSrc.APP_PROD_ID) ELSE "")) = '' THEN 'Y' ELSE  if Trim(( IF IsNotNull((FrmSrc.APP_PROD_ID)) THEN (FrmSrc.APP_PROD_ID) ELSE "")) = 0 THEN 'Y' Else 'N',
		IFF(
	    {{ ref('Src_Com_Bus_App_Prod_Client_Role') }}.APP_PROD_ID IS NULL, 'Y', 
	    IFF(TRIM(IFF({{ ref('Src_Com_Bus_App_Prod_Client_Role') }}.APP_PROD_ID IS NOT NULL, {{ ref('Src_Com_Bus_App_Prod_Client_Role') }}.APP_PROD_ID, '')) = '', 'Y', IFF(TRIM(IFF({{ ref('Src_Com_Bus_App_Prod_Client_Role') }}.APP_PROD_ID IS NOT NULL, {{ ref('Src_Com_Bus_App_Prod_Client_Role') }}.APP_PROD_ID, '')) = 0, 'Y', 'N'))
	) AS svIsNullAppProdId,
		-- *SRC*: \(20)If IsNull(FrmSrc.CIF_CODE) Then 'Y' Else  if Trim(( IF IsNotNull((FrmSrc.CIF_CODE)) THEN (FrmSrc.CIF_CODE) ELSE "")) = '' THEN 'Y' ELSE  if trim(FrmSrc.CIF_CODE) = '0' then 'Y' else 'N',
		IFF({{ ref('Src_Com_Bus_App_Prod_Client_Role') }}.CIF_CODE IS NULL, 'Y', IFF(TRIM(IFF({{ ref('Src_Com_Bus_App_Prod_Client_Role') }}.CIF_CODE IS NOT NULL, {{ ref('Src_Com_Bus_App_Prod_Client_Role') }}.CIF_CODE, '')) = '', 'Y', IFF(TRIM({{ ref('Src_Com_Bus_App_Prod_Client_Role') }}.CIF_CODE) = '0', 'Y', 'N'))) AS svIsNullCifCode,
		-- *SRC*: \(20)If IsNull(FrmSrc.ROLE_CAT_ID) Then 'Y' Else  If Trim(FrmSrc.ROLE_CAT_ID) = '' Then 'Y' Else 'N',
		IFF({{ ref('Src_Com_Bus_App_Prod_Client_Role') }}.ROLE_CAT_ID IS NULL, 'Y', IFF(TRIM({{ ref('Src_Com_Bus_App_Prod_Client_Role') }}.ROLE_CAT_ID) = '', 'Y', 'N')) AS svIsNullRolCatId,
		SUBTYPE_CODE,
		CIF_CODE,
		APP_PROD_ID,
		ROLE_CAT_ID,
		svIsNullRolCatId AS ROLE_CAT_ID_CHK,
		APP_PROD_CLIENT_ROLE_ID,
		-- *SRC*: SetNull(),
		SETNULL() AS PATY_ROLE_C
	FROM {{ ref('Src_Com_Bus_App_Prod_Client_Role') }}
	WHERE svIsNullAppProdId = 'N' AND svIsNullCifCode = 'N'
)

SELECT * FROM Transformer