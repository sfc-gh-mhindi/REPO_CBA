{{ config(materialized='view', tags=['XfmCPL_APP_PROD_Appt_Pdct_FeatExt']) }}

WITH Modify_226 AS (
	SELECT 
	--Manual
	--APPT_QLFY_C: string[max=2]= handle_null (APPT_QLFY_C, '99')
	--FEAT_I: string[max=4]= handle_null (FEAT_I, '9999')
	LOAN_SBTY_CODE AS APP_PROD_ID, APPT_I AS CAMPAIGN_CAT_ID, CLP_APP_PROD_ID AS ORIG_ETL_D, APPT_QLFY_C, FEAT_I, ACTL_VAL_R 
	FROM {{ ref('LkpReferences') }}
)

SELECT * FROM Modify_226