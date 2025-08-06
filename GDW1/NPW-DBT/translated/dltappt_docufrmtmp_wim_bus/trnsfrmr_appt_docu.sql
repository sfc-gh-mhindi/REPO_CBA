{{ config(materialized='view', tags=['DltAPPT_DOCUFrmTMP_WIM_BUS']) }}

WITH Trnsfrmr_Appt_Docu AS (
	SELECT
		-- *SRC*: \(20)If IsNull(ToTransfrmr.OLD_APPT_I) then 'Y' else 'N',
		IFF({{ ref('Join_Tmp_Wim_Bus_Evnt_Tables') }}.OLD_APPT_I IS NULL, 'Y', 'N') AS svOldApptI,
		{{ ref('Join_Tmp_Wim_Bus_Evnt_Tables') }}.NEW_APPT_I AS APPT_I,
		'BRPK' AS DOCU_C,
		'CSE' AS SRCE_SYST_C,
		'R24A' AS DOCU_VERS_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		'9999-12-31' AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		0 AS ROW_SECU_ACCS_C
	FROM {{ ref('Join_Tmp_Wim_Bus_Evnt_Tables') }}
	WHERE svOldApptI = 'Y'
)

SELECT * FROM Trnsfrmr_Appt_Docu