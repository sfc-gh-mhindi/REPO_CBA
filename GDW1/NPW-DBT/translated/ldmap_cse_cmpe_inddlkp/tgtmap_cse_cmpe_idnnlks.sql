{{ config(materialized='incremental', alias='_cba__app_hlt_dev_lookupset_map__cse__smpe__indd', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_CMPE_INDDLkp']) }}

SELECT
	INSN_ID,
	CMPE_I 
FROM {{ ref('XfmConversions') }}