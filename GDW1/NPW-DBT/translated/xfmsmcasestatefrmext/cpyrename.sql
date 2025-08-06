{{ config(materialized='view', tags=['XfmSmCaseStateFrmExt']) }}

WITH CpyRename AS (
	SELECT
		SM_CASE_STATE_ID,
		SM_CASE_ID,
		SM_STATE_CAT_ID,
		START_DATE,
		END_DATE,
		CREATED_BY_STAFF_NUMBER,
		STATE_CAUSED_BY_ACTION_ID,
		ORIG_ETL_D
	FROM {{ ref('SrcSmCaseStatePremapDS') }}
)

SELECT * FROM CpyRename