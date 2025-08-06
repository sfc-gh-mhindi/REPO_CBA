{{ config(materialized='incremental', alias='tmp_rm_rate', incremental_strategy='append', tags=['LdTmp_Rm_Rate_Paty']) }}

SELECT
	PATY_I 
FROM {{ ref('XfmTrans') }}