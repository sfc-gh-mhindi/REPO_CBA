{{ config(materialized='view', tags=['XfmCPL_APP_PROD_Appt_Pdct_FeatExt']) }}

WITH XfmBusinessRules__REJ AS (
	SELECT
		-- *SRC*: \(20)If InXfmBusinessRules.APPT_QLFY_C = '99' then 'REJ792' else 'L',
		IFF({{ ref('Modify_226') }}.APPT_QLFY_C = '99', 'REJ792', 'L') AS svErrorCode,
		-- *SRC*: \(20)If InXfmBusinessRules.FEAT_I = '9999' then 'REJ793' else 'L',
		IFF({{ ref('Modify_226') }}.FEAT_I = '9999', 'REJ793', 'L') AS svErrorCode1,
		-- *SRC*: \(20)If svErrorCode = "L" AND svErrorCode1 = "L" Then 'Y' Else 'N',
		IFF(svErrorCode = 'L' AND svErrorCode1 = 'L', 'Y', 'N') AS svRejFlag,
		APP_PROD_ID,
		{{ ref('Modify_226') }}.APPT_QLFY_C AS COM_SUBTYPE_CODE,
		CAMPAIGN_CAT_ID,
		'NOT REQD' AS COM_APP_ID,
		ETL_PROCESS_DT AS ETL_D,
		ORIG_ETL_D,
		'RPR792793' AS EROR_C
	FROM {{ ref('Modify_226') }}
	WHERE svRejFlag = 'N'
)

SELECT * FROM XfmBusinessRules__REJ