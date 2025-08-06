{{ config(materialized='view', tags=['XfmComBusSmCaseFrmExt']) }}

WITH ModNullHandling AS (
	SELECT 
	--Manual
	--TARG_I: string[max=255]= handle_null (TARG_I, '9999')
	--TARG_SUBJ: string[max=255]= handle_null (TARG_SUBJ, '9999')
	SM_CASE_ID, CREATED_TIMESTAMP, WIM_PROCESS_ID, ORIG_ETL_D, TARG_I, TARG_SUBJ 
	FROM {{ ref('LkpReferences') }}
)

SELECT * FROM ModNullHandling