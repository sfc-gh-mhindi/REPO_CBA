{{ config(materialized='incremental', alias='tmp_rm_rate', incremental_strategy='append', tags=['LdTmp_Rm_Rate_Evnt']) }}

SELECT
	EVNT_I
	WIM_PROCESS_ID 
FROM {{ ref('XfmTrans') }}