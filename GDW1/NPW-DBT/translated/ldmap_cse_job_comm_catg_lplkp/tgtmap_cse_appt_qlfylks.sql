{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__dev_lookupset_map__cse__job__comm__catg__code', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_JOB_COMM_CATG_LPLkp']) }}

SELECT
	CLP_JOB_FAMILY_CAT_ID,
	JOB_COMM_CATG_C 
FROM {{ ref('XfmConversions') }}