{{ config(materialized='incremental', alias='_cba__app_csel4_prod_lookupset_map__cse__sm__case__sm__case__id__lkp', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_SM_CASE_Lukp_Ld']) }}

SELECT
	SM_CASE_ID,
	TARG_I,
	TARG_SUBJ 
FROM {{ ref('XfmConversions') }}