{{ config(materialized='view', tags=['XfmAppt_Pdct_Dept']) }}

WITH NullRec_Nom AS (
	SELECT
		-- *SRC*: \(20)if IsNull(RemoveNullRecNom.APP_PROD_ID) then 'Y' Else  if trim(RemoveNullRecNom.APP_PROD_ID) = '' then 'Y' Else  if trim(RemoveNullRecNom.APP_PROD_ID) = 0 Then 'Y' ELSE 'N',
		IFF({{ ref('Copy') }}.APP_PROD_ID IS NULL, 'Y', IFF(TRIM({{ ref('Copy') }}.APP_PROD_ID) = '', 'Y', IFF(TRIM({{ ref('Copy') }}.APP_PROD_ID) = 0, 'Y', 'N'))) AS IsNullAppProdId,
		-- *SRC*: \(20)if IsNull(RemoveNullRecNom.NOMINATED_BSB) Then 'Y' Else  if trim(RemoveNullRecNom.NOMINATED_BSB) = '' Then 'Y' ELSE  if trim(RemoveNullRecNom.NOMINATED_BSB) = 0 Then 'Y' Else 'N',
		IFF({{ ref('Copy') }}.NOMINATED_BSB IS NULL, 'Y', IFF(TRIM({{ ref('Copy') }}.NOMINATED_BSB) = '', 'Y', IFF(TRIM({{ ref('Copy') }}.NOMINATED_BSB) = 0, 'Y', 'N'))) AS IsNullNominatedBSB,
		APP_PROD_ID,
		NOMINATED_BSB
	FROM {{ ref('Copy') }}
	WHERE IsNullAppProdId = 'N' AND IsNullNominatedBSB = 'N'
)

SELECT * FROM NullRec_Nom