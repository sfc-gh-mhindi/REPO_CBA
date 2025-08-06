{{ config(materialized='view', tags=['XfmAppt_Pdct_Rpay']) }}

WITH XfmNull AS (
	SELECT
		-- *SRC*: \(20)If IsNull(FrmSrc.PO_REPAYMENT_SOURCE_CAT_ID) Then 'Y' Else  if Trim(FrmSrc.PO_REPAYMENT_SOURCE_CAT_ID) = '' Then 'Y' Else  if Trim(FrmSrc.PO_REPAYMENT_SOURCE_CAT_ID) = 0 Then 'Y' Else 'N',
		IFF({{ ref('Src_CSE_CPO_BUS_APP_PROD') }}.PO_REPAYMENT_SOURCE_CAT_ID IS NULL, 'Y', IFF(TRIM({{ ref('Src_CSE_CPO_BUS_APP_PROD') }}.PO_REPAYMENT_SOURCE_CAT_ID) = '', 'Y', IFF(TRIM({{ ref('Src_CSE_CPO_BUS_APP_PROD') }}.PO_REPAYMENT_SOURCE_CAT_ID) = 0, 'Y', 'N'))) AS svIsNullPoRepaymentSourceCatId,
		-- *SRC*: \(20)If IsNull(FrmSrc.APP_PROD_ID) Then 'Y' Else  if Trim(FrmSrc.APP_PROD_ID) = '' Then 'Y' Else  if Trim(FrmSrc.APP_PROD_ID) = 0 Then 'Y' Else 'N',
		IFF({{ ref('Src_CSE_CPO_BUS_APP_PROD') }}.APP_PROD_ID IS NULL, 'Y', IFF(TRIM({{ ref('Src_CSE_CPO_BUS_APP_PROD') }}.APP_PROD_ID) = '', 'Y', IFF(TRIM({{ ref('Src_CSE_CPO_BUS_APP_PROD') }}.APP_PROD_ID) = 0, 'Y', 'N'))) AS svIsNullApprProdId,
		APP_PROD_ID,
		PO_REPAYMENT_SOURCE_CAT_ID,
		PO_REPAYMENT_SOURCE_OTHER
	FROM {{ ref('Src_CSE_CPO_BUS_APP_PROD') }}
	WHERE svIsNullPoRepaymentSourceCatId = 'N' AND svIsNullApprProdId = 'N'
)

SELECT * FROM XfmNull