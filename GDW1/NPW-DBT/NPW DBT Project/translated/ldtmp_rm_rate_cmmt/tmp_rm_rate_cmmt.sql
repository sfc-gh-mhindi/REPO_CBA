{{ config(materialized='incremental', alias='tmp_rm_rate_cmmt', incremental_strategy='append', tags=['LdTmp_Rm_Rate_Cmmt']) }}

SELECT
	ANTN_I
	EVNT_I 
FROM {{ ref('XfmTrans') }}