{{ config(materialized='view', tags=['DltMAP_CSE_SM_CASEFrmMAP_CSE_SM_CASE_DS']) }}

WITH XfmAddEfftDate AS (
	SELECT
		SM_CASE_ID,
		{{ ref('LuSmCaseId') }}.NEW_TARG_I AS TARG_I,
		{{ ref('LuSmCaseId') }}.NEW_TARG_SUBJ AS TARG_SUBJ,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D
	FROM {{ ref('LuSmCaseId') }}
	WHERE 
)

SELECT * FROM XfmAddEfftDate