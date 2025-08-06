{{ config(materialized='view', tags=['XfmHlmapp_Tpb']) }}

WITH XfmTrans__OutApptPdctPurp AS (
	SELECT
		-- *SRC*: \(20)IF IsNull(Totrns.CHL_AGENT_ALIAS_ID) THEN "N" ELSE  IF Trim(Totrns.CHL_AGENT_ALIAS_ID) = '' THEN "N" ELSE "Y",
		IFF({{ ref('MergeDS') }}.CHL_AGENT_ALIAS_ID IS NULL, 'N', IFF(TRIM({{ ref('MergeDS') }}.CHL_AGENT_ALIAS_ID) = '', 'N', 'Y')) AS svIsValidrecord1,
		-- *SRC*: \(20)IF IsNull(Totrns.HLM_APP_DISCHARGE_REASON_ID) THEN "N" ELSE  If Trim(Totrns.HLM_APP_DISCHARGE_REASON_ID) = '' THEN "N" ELSE "Y",
		IFF({{ ref('MergeDS') }}.HLM_APP_DISCHARGE_REASON_ID IS NULL, 'N', IFF(TRIM({{ ref('MergeDS') }}.HLM_APP_DISCHARGE_REASON_ID) = '', 'N', 'Y')) AS svIsValidrecord2,
		-- *SRC*: \(20)IF IsNull(Totrns.HLM_APP_PROD_ID) THEN "N" ELSE  If Trim(Totrns.HLM_APP_PROD_ID) = '' THEN "N" ELSE "Y",
		IFF({{ ref('MergeDS') }}.HLM_APP_PROD_ID IS NULL, 'N', IFF(TRIM({{ ref('MergeDS') }}.HLM_APP_PROD_ID) = '', 'N', 'Y')) AS svIsValidrecord3,
		-- *SRC*: \(20)If (Trim(( IF IsNotNull((Totrns.HLM_APP_PROD_ID)) THEN (Totrns.HLM_APP_PROD_ID) ELSE 0)) = 0) Then 'N' else  if Trim(Totrns.HLM_APP_PROD_ID) = '' Then 'N' else  if num(Totrns.HLM_APP_PROD_ID) then ( if (StringToDecimal(TRIM(Totrns.HLM_APP_PROD_ID)) = 0) Then 'N' else 'Y') else 'Y',
		IFF(TRIM(IFF({{ ref('MergeDS') }}.HLM_APP_PROD_ID IS NOT NULL, {{ ref('MergeDS') }}.HLM_APP_PROD_ID, 0)) = 0, 'N', IFF(TRIM({{ ref('MergeDS') }}.HLM_APP_PROD_ID) = '', 'N', IFF(NUM({{ ref('MergeDS') }}.HLM_APP_PROD_ID), IFF(STRINGTODECIMAL(TRIM({{ ref('MergeDS') }}.HLM_APP_PROD_ID)) = 0, 'N', 'Y'), 'Y'))) AS svIsValidrecord4,
		-- *SRC*: \(20)If (Trim(( IF IsNotNull((Totrns.PEXA_FLAG)) THEN (Totrns.PEXA_FLAG) ELSE 0)) = 0) Then 'N' else  if Trim(Totrns.PEXA_FLAG) = '' Then 'N' else  if num(Totrns.PEXA_FLAG) then ( if (StringToDecimal(TRIM(Totrns.PEXA_FLAG)) = 0) Then 'N' else 'Y') else 'Y',
		IFF(TRIM(IFF({{ ref('MergeDS') }}.PEXA_FLAG IS NOT NULL, {{ ref('MergeDS') }}.PEXA_FLAG, 0)) = 0, 'N', IFF(TRIM({{ ref('MergeDS') }}.PEXA_FLAG) = '', 'N', IFF(NUM({{ ref('MergeDS') }}.PEXA_FLAG), IFF(STRINGTODECIMAL(TRIM({{ ref('MergeDS') }}.PEXA_FLAG)) = 0, 'N', 'Y'), 'Y'))) AS svlsValidPexachk,
		APP_ID,
		HLM_APP_PROD_ID,
		{{ ref('MergeDS') }}.HLM_APP__ACCOUNT_ID AS HLM_ACCOUNT_ID,
		{{ ref('MergeDS') }}.HLM_APP_ACCOUNT_NUMBER AS ACCOUNT_NUMBER,
		{{ ref('MergeDS') }}.HLM_APP_CRIS_PRODUCT_ID AS CRIS_PRODUCT_ID,
		HLM_APP_TYPE_CAT_ID,
		{{ ref('MergeDS') }}.HLM_APP_DISCHARGE_REASON_ID AS DCHG_REAS_ID,
		{{ ref('MergeDS') }}.HLM_APP_PROD_ID AS HL_APP_PROD_ID,
		PEXA_FLAG,
		RUN_STREAM AS RUN_STRM
	FROM {{ ref('MergeDS') }}
	WHERE svIsValidrecord2 = 'Y' AND svIsValidrecord3 = 'Y'
)

SELECT * FROM XfmTrans__OutApptPdctPurp