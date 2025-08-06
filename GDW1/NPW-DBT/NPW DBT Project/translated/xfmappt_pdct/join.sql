{{ config(materialized='view', tags=['XfmAppt_Pdct']) }}

WITH Join AS (
	SELECT
		{{ ref('SrcCseComBusAppProd') }}.APP_PROD_ID,
		{{ ref('SrcCseComBusAppProd') }}.APP_ID,
		{{ ref('SrcCseComBusAppProd') }}.SUBTYPE_CODE,
		{{ ref('SrcCseComBusAppProd') }}.PDCT_N,
		{{ ref('SrcCseCpoBusAppProd') }}.PO_OVERDRAFT_CAT_ID
	FROM {{ ref('SrcCseComBusAppProd') }}
	OUTER JOIN {{ ref('SrcCseCpoBusAppProd') }} ON {{ ref('SrcCseComBusAppProd') }}.APP_PROD_ID = {{ ref('SrcCseCpoBusAppProd') }}.APP_PROD_ID
)

SELECT * FROM Join