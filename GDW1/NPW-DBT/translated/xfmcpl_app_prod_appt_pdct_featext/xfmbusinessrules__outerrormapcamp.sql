{{ config(materialized='view', tags=['XfmCPL_APP_PROD_Appt_Pdct_FeatExt']) }}

WITH XfmBusinessRules__OutErrorMapCamp AS (
	SELECT
		-- *SRC*: \(20)If InXfmBusinessRules.APPT_QLFY_C = '99' then 'REJ792' else 'L',
		IFF({{ ref('Modify_226') }}.APPT_QLFY_C = '99', 'REJ792', 'L') AS svErrorCode,
		-- *SRC*: \(20)If InXfmBusinessRules.FEAT_I = '9999' then 'REJ793' else 'L',
		IFF({{ ref('Modify_226') }}.FEAT_I = '9999', 'REJ793', 'L') AS svErrorCode1,
		-- *SRC*: \(20)If svErrorCode = "L" AND svErrorCode1 = "L" Then 'Y' Else 'N',
		IFF(svErrorCode = 'L' AND svErrorCode1 = 'L', 'Y', 'N') AS svRejFlag,
		{{ ref('Modify_226') }}.APP_PROD_ID AS SRCE_KEY_I,
		'CSE_CLP_BUS_APPT_PDCT_FEAT' AS CONV_M,
		'LOOKUP' AS CONV_MAP_RULE_M,
		'MAP_CSE_CAMP_CAT' AS TRSF_TABL_M,
		{{ ref('Modify_226') }}.CAMPAIGN_CAT_ID AS VALU_CHNG_BFOR_X,
		{{ ref('Modify_226') }}.CAMPAIGN_CAT_ID AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name = ' : DSJobName,
		CONCAT('Job Name = ', DSJobName) AS TRSF_X,
		'CAMPAIGN_CAT_ID' AS TRSF_COLM_M
	FROM {{ ref('Modify_226') }}
	WHERE svErrorCode1 = 'REJ793'
)

SELECT * FROM XfmBusinessRules__OutErrorMapCamp