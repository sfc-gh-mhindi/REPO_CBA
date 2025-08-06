{{ config(materialized='view', tags=['XfmHL_APPFrmExt_NEW']) }}

WITH XfmBusinessRules__OutTmpCseApptGnrcDate AS (
	SELECT
		-- *SRC*: 'CSE' : 'HL' : Trim(InXfmBusinessRules.HL_APP_ID),
		CONCAT(CONCAT('CSE', 'HL'), TRIM({{ ref('CpyRename') }}.HL_APP_ID)) AS APPT_I,
		'SETL' AS DATE_ROLE_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: SetNull(),
		SETNULL() AS GNRC_ROLE_S,
		-- *SRC*: \(20)If IsNull(InXfmBusinessRules.SETTLEMENT_DATE) Then StringToDate(DEFAULT_DT, "%yyyy%mm%dd") Else  If IsValid('date', StringToDate(InXfmBusinessRules.SETTLEMENT_DATE, "%yyyy%mm%dd")) Then StringToDate(InXfmBusinessRules.SETTLEMENT_DATE, "%yyyy%mm%dd") Else StringToDate(DEFAULT_DT, "%yyyy%mm%dd"),
		IFF({{ ref('CpyRename') }}.SETTLEMENT_DATE IS NULL, STRINGTODATE(DEFAULT_DT, '%yyyy%mm%dd'), IFF(ISVALID('date', STRINGTODATE({{ ref('CpyRename') }}.SETTLEMENT_DATE, '%yyyy%mm%dd')), STRINGTODATE({{ ref('CpyRename') }}.SETTLEMENT_DATE, '%yyyy%mm%dd'), STRINGTODATE(DEFAULT_DT, '%yyyy%mm%dd'))) AS GNRC_ROLE_D,
		-- *SRC*: SetNull(),
		SETNULL() AS GNRC_ROLE_T,
		-- *SRC*: StringToDate('99991231', '%yyyy%mm%dd'),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		-- *SRC*: SetNull(),
		SETNULL() AS MODF_S,
		-- *SRC*: SetNull(),
		SETNULL() AS MODF_D,
		-- *SRC*: SetNull(),
		SETNULL() AS MODF_T,
		-- *SRC*: SetNull(),
		SETNULL() AS USER_I,
		-- *SRC*: SetNull(),
		SETNULL() AS CHNG_REAS_TYPE_C
	FROM {{ ref('CpyRename') }}
	WHERE 
)

SELECT * FROM XfmBusinessRules__OutTmpCseApptGnrcDate