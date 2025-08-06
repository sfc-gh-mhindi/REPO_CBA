{{ config(materialized='view', tags=['XfmHlmapp_Tpb']) }}

WITH Transformer_32__toerr_RQM3 AS (
	SELECT
		-- *SRC*: \(20)IF IsNull(frmlkp.PURP_TYPE_C) THEN "N" elSE "Y",
		IFF({{ ref('LkpDateRoleC') }}.PURP_TYPE_C IS NULL, 'N', 'Y') AS svlsValidRecord,
		-- *SRC*: \(20)If (Trim(( IF IsNotNull((frmlkp.HLM_APP_PROD_ID)) THEN (frmlkp.HLM_APP_PROD_ID) ELSE 0)) = 0) Then 'N' else  if Trim(frmlkp.HLM_APP_PROD_ID) = '' Then 'N' else  if num(frmlkp.HLM_APP_PROD_ID) then ( if (StringToDecimal(TRIM(frmlkp.HLM_APP_PROD_ID)) = 0) Then 'N' else 'Y') else 'Y',
		IFF(TRIM(IFF({{ ref('LkpDateRoleC') }}.HLM_APP_PROD_ID IS NOT NULL, {{ ref('LkpDateRoleC') }}.HLM_APP_PROD_ID, 0)) = 0, 'N', IFF(TRIM({{ ref('LkpDateRoleC') }}.HLM_APP_PROD_ID) = '', 'N', IFF(NUM({{ ref('LkpDateRoleC') }}.HLM_APP_PROD_ID), IFF(STRINGTODECIMAL(TRIM({{ ref('LkpDateRoleC') }}.HLM_APP_PROD_ID)) = 0, 'N', 'Y'), 'Y'))) AS svhlmapppdctid,
		{{ ref('LkpDateRoleC') }}.HL_APP_PROD_ID AS SRCE_KEY_I,
		GDW_USER AS CONV_M,
		'Lookup failure on MAP_CSE_APPT_PDCT_PURP_HM table' AS CONV_MAP_RULE_M,
		'APPT_PDCT_PURP' AS TRSF_TABL_M,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS SRCE_EFFT_D,
		-- *SRC*: \(20)IF IsNull(frmlkp.PURP_TYPE_C) THEN SetNull() ELSE frmlkp.PURP_TYPE_C,
		IFF({{ ref('LkpDateRoleC') }}.PURP_TYPE_C IS NULL, SETNULL(), {{ ref('LkpDateRoleC') }}.PURP_TYPE_C) AS VALU_CHNG_BFOR_X,
		'9999' AS VALU_CHNG_AFTR_X,
		DsjobName AS TRSF_X,
		'PURP_TYPE_C' AS TRSF_COLM_M,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		'CSE_CHL_BUS_HLMAPP' AS SRCE_FILE_M,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: "CSEHM" : frmlkp.HL_APP_PROD_ID,
		CONCAT('CSEHM', {{ ref('LkpDateRoleC') }}.HL_APP_PROD_ID) AS TRSF_KEY_I
	FROM {{ ref('LkpDateRoleC') }}
	WHERE svlsValidRecord = 'N'
)

SELECT * FROM Transformer_32__toerr_RQM3