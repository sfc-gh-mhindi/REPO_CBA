{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__loan__term__pl__prod__term__cat__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_LOAN_TERM_PL_Lkp']) }}

SELECT
	PL_PROD_TERM_CAT_ID,
	SRCE_SYST_ACTL_TERM_Q 
FROM {{ ref('XfmConversions') }}