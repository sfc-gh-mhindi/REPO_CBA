{{ config(materialized='view', tags=['XfmAppt_Pdct_Dept']) }}

WITH NullRec_Logn AS (
	SELECT
		-- *SRC*: \(20)if IsNull(RemoveNullRecLogn.APP_PROD_ID) then 'Y' Else  if trim(RemoveNullRecLogn.APP_PROD_ID) = '' then 'Y' Else  if RemoveNullRecLogn.APP_PROD_ID = 0 Then 'Y' ELSE 'N',
		IFF({{ ref('Copy') }}.APP_PROD_ID IS NULL, 'Y', IFF(TRIM({{ ref('Copy') }}.APP_PROD_ID) = '', 'Y', IFF({{ ref('Copy') }}.APP_PROD_ID = 0, 'Y', 'N'))) AS IsNullAppProdId,
		-- *SRC*: \(20)if IsNull(RemoveNullRecLogn.LODGEMENT_BRANCH_BSB) Then 'Y' Else  if trim(RemoveNullRecLogn.LODGEMENT_BRANCH_BSB) = '' then 'Y' ELSE  if trim(RemoveNullRecLogn.LODGEMENT_BRANCH_BSB) = 0 Then 'Y' Else 'N',
		IFF({{ ref('Copy') }}.LODGEMENT_BRANCH_BSB IS NULL, 'Y', IFF(TRIM({{ ref('Copy') }}.LODGEMENT_BRANCH_BSB) = '', 'Y', IFF(TRIM({{ ref('Copy') }}.LODGEMENT_BRANCH_BSB) = 0, 'Y', 'N'))) AS IsNullLodgementBranchBSB,
		APP_PROD_ID,
		LODGEMENT_BRANCH_BSB
	FROM {{ ref('Copy') }}
	WHERE IsNullAppProdId = 'N' AND IsNullLodgementBranchBSB = 'N'
)

SELECT * FROM NullRec_Logn