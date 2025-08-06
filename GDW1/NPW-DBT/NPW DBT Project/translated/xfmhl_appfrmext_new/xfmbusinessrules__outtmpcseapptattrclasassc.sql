{{ config(materialized='view', tags=['XfmHL_APPFrmExt_NEW']) }}

WITH XfmBusinessRules__OutTmpCseApptAttrClasAssc AS (
	SELECT
		-- *SRC*: 'CSE' : 'HL' : Trim(InXfmBusinessRules.HL_APP_ID),
		CONCAT(CONCAT('CSE', 'HL'), TRIM({{ ref('CpyRename') }}.HL_APP_ID)) AS APPT_I,
		'FHBF' AS APPT_ATTR_CLAS_C,
		{{ ref('CpyRename') }}.FHB_FLAG AS APPT_ATTR_CLAS_VALU_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		'CSE' AS SRCE_SYST_C,
		-- *SRC*: StringToDate('99991231', '%yyyy%mm%dd'),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		0 AS ROW_SECU_ACCS_C
	FROM {{ ref('CpyRename') }}
	WHERE SUBSTRING({{ ref('CpyRename') }}.FHB_FLAG, 1, 1) = 'N' OR SUBSTRING({{ ref('CpyRename') }}.FHB_FLAG, 1, 1) = 'Y'
)

SELECT * FROM XfmBusinessRules__OutTmpCseApptAttrClasAssc