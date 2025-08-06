{{ config(materialized='view', tags=['XfmHlmapp_Tpb']) }}

WITH Transformer_32__OutApptPdctPurp AS (
	SELECT
		-- *SRC*: \(20)IF IsNull(frmlkp.PURP_TYPE_C) THEN "N" elSE "Y",
		IFF({{ ref('LkpDateRoleC') }}.PURP_TYPE_C IS NULL, 'N', 'Y') AS svlsValidRecord,
		-- *SRC*: \(20)If (Trim(( IF IsNotNull((frmlkp.HLM_APP_PROD_ID)) THEN (frmlkp.HLM_APP_PROD_ID) ELSE 0)) = 0) Then 'N' else  if Trim(frmlkp.HLM_APP_PROD_ID) = '' Then 'N' else  if num(frmlkp.HLM_APP_PROD_ID) then ( if (StringToDecimal(TRIM(frmlkp.HLM_APP_PROD_ID)) = 0) Then 'N' else 'Y') else 'Y',
		IFF(TRIM(IFF({{ ref('LkpDateRoleC') }}.HLM_APP_PROD_ID IS NOT NULL, {{ ref('LkpDateRoleC') }}.HLM_APP_PROD_ID, 0)) = 0, 'N', IFF(TRIM({{ ref('LkpDateRoleC') }}.HLM_APP_PROD_ID) = '', 'N', IFF(NUM({{ ref('LkpDateRoleC') }}.HLM_APP_PROD_ID), IFF(STRINGTODECIMAL(TRIM({{ ref('LkpDateRoleC') }}.HLM_APP_PROD_ID)) = 0, 'N', 'Y'), 'Y'))) AS svhlmapppdctid,
		-- *SRC*: "CSEHM" : frmlkp.HL_APP_PROD_ID,
		CONCAT('CSEHM', {{ ref('LkpDateRoleC') }}.HL_APP_PROD_ID) AS APPT_PDCT_I,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: \(20)IF IsNull(frmlkp.DCHG_REAS_ID) THEN ( IF IsNotNull((frmlkp.DCHG_REAS_ID)) THEN (frmlkp.DCHG_REAS_ID) ELSE "") ELSE frmlkp.DCHG_REAS_ID,
		IFF({{ ref('LkpDateRoleC') }}.DCHG_REAS_ID IS NULL, IFF({{ ref('LkpDateRoleC') }}.DCHG_REAS_ID IS NOT NULL, {{ ref('LkpDateRoleC') }}.DCHG_REAS_ID, ''), {{ ref('LkpDateRoleC') }}.DCHG_REAS_ID) AS SRCE_SYST_APPT_PDCT_PURP_I,
		-- *SRC*: \(20)IF IsNull(frmlkp.PURP_TYPE_C) THEN '9999' ELSE frmlkp.PURP_TYPE_C,
		IFF({{ ref('LkpDateRoleC') }}.PURP_TYPE_C IS NULL, '9999', {{ ref('LkpDateRoleC') }}.PURP_TYPE_C) AS PURP_TYPE_C,
		-- *SRC*: SetNull(),
		SETNULL() AS PURP_CLAS_C,
		'CSE' AS SRCE_SYST_C,
		-- *SRC*: SetNull(),
		SETNULL() AS PURP_A,
		-- *SRC*: SetNull(),
		SETNULL() AS CNCY_C,
		-- *SRC*: SetNull(),
		SETNULL() AS MAIN_PURP_F,
		-- *SRC*: StringToDate("99991231", "%yyyy%mm%dd"),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		RUN_STREAM AS RUN_STRM
	FROM {{ ref('LkpDateRoleC') }}
	WHERE 
)

SELECT * FROM Transformer_32__OutApptPdctPurp