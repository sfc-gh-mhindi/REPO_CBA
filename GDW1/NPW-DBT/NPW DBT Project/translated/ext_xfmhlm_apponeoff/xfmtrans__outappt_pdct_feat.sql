{{ config(materialized='view', tags=['Ext_XfmHLM_APPONEOFF']) }}

WITH XfmTrans__outAppt_Pdct_Feat AS (
	SELECT
		-- *SRC*: \(20)If (Trim(( IF IsNotNull((outFeatValucode.HLM_APP_PROD_ID)) THEN (outFeatValucode.HLM_APP_PROD_ID) ELSE 0)) = 0) Then 'N' else  if Trim(outFeatValucode.HLM_APP_PROD_ID) = '' Then 'N' else  if num(outFeatValucode.HLM_APP_PROD_ID) then ( if (StringToDecimal(TRIM(outFeatValucode.HLM_APP_PROD_ID)) = 0) Then 'N' else 'Y') else 'Y',
		IFF(
	    TRIM(IFF({{ ref('LkpMapCseApptPDctFeatValueC') }}.HLM_APP_PROD_ID IS NOT NULL, {{ ref('LkpMapCseApptPDctFeatValueC') }}.HLM_APP_PROD_ID, 0)) = 0, 'N', 
	    IFF(TRIM({{ ref('LkpMapCseApptPDctFeatValueC') }}.HLM_APP_PROD_ID) = '', 'N', IFF(NUM({{ ref('LkpMapCseApptPDctFeatValueC') }}.HLM_APP_PROD_ID), IFF(STRINGTODECIMAL(TRIM({{ ref('LkpMapCseApptPDctFeatValueC') }}.HLM_APP_PROD_ID)) = 0, 'N', 'Y'), 'Y'))
	) AS svIsValidrecord4,
		-- *SRC*: \(20)If (Trim(( IF IsNotNull((outFeatValucode.PEXA_FLAG)) THEN (outFeatValucode.PEXA_FLAG) ELSE 0)) = 0) Then 'N' else  if Trim(outFeatValucode.PEXA_FLAG) = '' Then 'N' else  if num(outFeatValucode.PEXA_FLAG) then ( if (StringToDecimal(TRIM(outFeatValucode.PEXA_FLAG)) = 0) Then 'N' else 'Y') else 'Y',
		IFF(
	    TRIM(IFF({{ ref('LkpMapCseApptPDctFeatValueC') }}.PEXA_FLAG IS NOT NULL, {{ ref('LkpMapCseApptPDctFeatValueC') }}.PEXA_FLAG, 0)) = 0, 'N', 
	    IFF(TRIM({{ ref('LkpMapCseApptPDctFeatValueC') }}.PEXA_FLAG) = '', 'N', IFF(NUM({{ ref('LkpMapCseApptPDctFeatValueC') }}.PEXA_FLAG), IFF(STRINGTODECIMAL(TRIM({{ ref('LkpMapCseApptPDctFeatValueC') }}.PEXA_FLAG)) = 0, 'N', 'Y'), 'Y'))
	) AS svlsValidPexachk,
		-- *SRC*: "CSEHM" : outFeatValucode.HLM_APP_PROD_ID,
		CONCAT('CSEHM', {{ ref('LkpMapCseApptPDctFeatValueC') }}.HLM_APP_PROD_ID) AS APPT_PDCT_I,
		'2789' AS FEAT_I,
		'NOT APPLICABLE' AS SRCE_SYST_APPT_FEAT_I,
		-- *SRC*: StringToDate(Trim(ETL_PROCESS_DT[1, 4]) : '-' : Trim(ETL_PROCESS_DT[5, 2]) : '-' : Trim(ETL_PROCESS_DT[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(ETL_PROCESS_DT, 1, 4)), '-'), TRIM(SUBSTRING(ETL_PROCESS_DT, 5, 2))), '-'), TRIM(SUBSTRING(ETL_PROCESS_DT, 7, 2))), '%yyyy-%mm-%dd') AS EFFT_D,
		'CSE' AS SRCE_SYST_C,
		-- *SRC*: SetNull(),
		SETNULL() AS SRCE_SYST_APPT_OVRD_I,
		-- *SRC*: SetNull(),
		SETNULL() AS OVRD_FEAT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS SRCE_SYST_STND_VALU_Q,
		-- *SRC*: SetNull(),
		SETNULL() AS SRCE_SYST_STND_VALU_R,
		-- *SRC*: SetNull(),
		SETNULL() AS SRCE_SYST_STND_VALU_A,
		-- *SRC*: SetNull(),
		SETNULL() AS CNCY_C,
		-- *SRC*: SetNull(),
		SETNULL() AS ACTL_VALU_Q,
		-- *SRC*: SetNull(),
		SETNULL() AS ACTL_VALU_R,
		-- *SRC*: SetNull(),
		SETNULL() AS ACTL_VALU_A,
		-- *SRC*: SetNull(),
		SETNULL() AS FEAT_SEQN_N,
		-- *SRC*: SetNull(),
		SETNULL() AS FEAT_STRT_D,
		-- *SRC*: SetNull(),
		SETNULL() AS FEE_CHRG_D,
		-- *SRC*: SetNull(),
		SETNULL() AS OVRD_REAS_C,
		-- *SRC*: SetNull(),
		SETNULL() AS FEE_ADD_TO_TOTL_F,
		-- *SRC*: SetNull(),
		SETNULL() AS FEE_CAPL_F,
		-- *SRC*: StringToDate("99991231", "%yyyy%mm%dd"),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		0 AS ROW_SECU_ACCS_C,
		PEXA_FLAG,
		FEAT_VALU_C,
		RUN_STREAM AS RUN_STRM
	FROM {{ ref('LkpMapCseApptPDctFeatValueC') }}
	WHERE svIsValidrecord4 = 'Y' AND svlsValidPexachk = 'Y' AND TRIMLEADINGTRAILING(IFF({{ ref('LkpMapCseApptPDctFeatValueC') }}.PEXA_FLAG IS NOT NULL, {{ ref('LkpMapCseApptPDctFeatValueC') }}.PEXA_FLAG, '')) <> 'N'
)

SELECT * FROM XfmTrans__outAppt_Pdct_Feat