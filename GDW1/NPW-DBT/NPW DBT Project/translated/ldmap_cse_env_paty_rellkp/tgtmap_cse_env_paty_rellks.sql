{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__env__paty__rel__clnt__posn__clnt__reln__type__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_ENV_PATY_RELLkp']) }}

SELECT
	CLIENT_RELATIONSHIP_TYPE_ID,
	CLIENT_POSITION,
	REL_C 
FROM {{ ref('XfmConversions') }}