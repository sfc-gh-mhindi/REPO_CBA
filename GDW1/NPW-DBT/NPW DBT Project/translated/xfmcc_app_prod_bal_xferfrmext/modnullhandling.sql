{{ config(materialized='view', tags=['XfmCC_APP_PROD_BAL_XFERFrmExt']) }}

WITH ModNullHandling AS (
	SELECT 
	--Manual
	--TRNF_OPTN_C: string[max=4]= handle_null (TRNF_OPTN_C, '9999')
	--CMPE_I: string[max=255]= handle_null (CMPE_I, '9999')
	CC_APP_PROD_BAL_XFER_ID, BAL_XFER_OPTION_CAT_ID, XFER_AMT, BAL_XFER_INSTITUTION_CAT_ID, CC_APP_PROD_ID, CC_APP_ID, ORIG_ETL_D, TRNF_OPTN_C, CMPE_I 
	FROM {{ ref('LkpReferences') }}
)

SELECT * FROM ModNullHandling