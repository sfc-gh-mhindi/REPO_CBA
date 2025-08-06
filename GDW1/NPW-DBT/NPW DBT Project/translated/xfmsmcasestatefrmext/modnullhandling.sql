{{ config(materialized='view', tags=['XfmSmCaseStateFrmExt']) }}

WITH ModNullHandling AS (
	SELECT 
	--Manual
	--STUS_C: string[4]= handle_null (STUS_C, '9999')
	--END_DATE: string[max=25]= handle_null (END_DATE, '9999')
	SM_CASE_STATE_ID, SM_CASE_ID, SM_STATE_CAT_ID, START_DATE, END_DATE, CREATED_BY_STAFF_NUMBER, STATE_CAUSED_BY_ACTION_ID, ORIG_ETL_D, targ_i, targ_tabl, STUS_C 
	FROM {{ ref('LkpReferences') }}
)

SELECT * FROM ModNullHandling