{{ config(materialized='view', tags=['XfmAppt']) }}

WITH XfmNull AS (
	SELECT
		-- *SRC*: \(20)if FrmSrc.SUBTYPE_CODE <> 'PO' THEN 'N' ELSE  If IsNull(FrmSrc.SUBTYPE_CODE) Then 'N' Else 'Y',
		IFF({{ ref('Src_CSE_COM_BUS_APP') }}.SUBTYPE_CODE <> 'PO', 'N', IFF({{ ref('Src_CSE_COM_BUS_APP') }}.SUBTYPE_CODE IS NULL, 'N', 'Y')) AS svIsNullSubtypeCode,
		-- *SRC*: \(20)If IsNull(FrmSrc.APP_ID) Then 'Y' Else  if Trim(FrmSrc.APP_ID) = '' Then 'Y' Else  if ( IF IsNotNull((FrmSrc.APP_ID)) THEN (FrmSrc.APP_ID) ELSE "") = '' Then 'Y' Else  if Trim(FrmSrc.APP_ID) = 0 Then 'Y' Else 'N',
		IFF({{ ref('Src_CSE_COM_BUS_APP') }}.APP_ID IS NULL, 'Y', IFF(TRIM({{ ref('Src_CSE_COM_BUS_APP') }}.APP_ID) = '', 'Y', IFF(IFF({{ ref('Src_CSE_COM_BUS_APP') }}.APP_ID IS NOT NULL, {{ ref('Src_CSE_COM_BUS_APP') }}.APP_ID, '') = '', 'Y', IFF(TRIM({{ ref('Src_CSE_COM_BUS_APP') }}.APP_ID) = 0, 'Y', 'N')))) AS svIsNullAppId,
		-- *SRC*: \(20)If IsNull(FrmSrc.CHANNEL_CAT_ID) Then 'Y' Else  If ( IF IsNotNull((FrmSrc.CHANNEL_CAT_ID)) THEN (FrmSrc.CHANNEL_CAT_ID) ELSE "") = '' Then 'Y' Else  if Trim(FrmSrc.CHANNEL_CAT_ID) = 0 Then 'Y' Else 'N',
		IFF({{ ref('Src_CSE_COM_BUS_APP') }}.CHANNEL_CAT_ID IS NULL, 'Y', IFF(IFF({{ ref('Src_CSE_COM_BUS_APP') }}.CHANNEL_CAT_ID IS NOT NULL, {{ ref('Src_CSE_COM_BUS_APP') }}.CHANNEL_CAT_ID, '') = '', 'Y', IFF(TRIM({{ ref('Src_CSE_COM_BUS_APP') }}.CHANNEL_CAT_ID) = 0, 'Y', 'N'))) AS svIsNullChannelCatId,
		APP_ID,
		CHANNEL_CAT_ID,
		APP_NO,
		CREATED_DATE,
		APP_ENTRY_POINT,
		SUBTYPE_CODE,
		svIsNullChannelCatId AS CHANNEL_CAT_ID_CHK,
		svIsNullChannelCatId AS APPT_ORIG_C
	FROM {{ ref('Src_CSE_COM_BUS_APP') }}
	WHERE svIsNullAppId = 'N' AND svIsNullSubtypeCode = 'Y'
)

SELECT * FROM XfmNull