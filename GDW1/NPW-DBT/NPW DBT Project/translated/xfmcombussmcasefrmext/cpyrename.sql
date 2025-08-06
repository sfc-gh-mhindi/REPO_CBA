{{ config(materialized='view', tags=['XfmComBusSmCaseFrmExt']) }}

WITH CpyRename AS (
	SELECT
		SM_CASE_ID,
		CREATED_TIMESTAMP,
		WIM_PROCESS_ID,
		ORIG_ETL_D
	FROM {{ ref('SrcPremapDS') }}
)

SELECT * FROM CpyRename