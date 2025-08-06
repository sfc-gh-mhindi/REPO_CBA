{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__unid__paty__catg__pl__tp__broker__group__cat__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_UNID_PATY_CATG_Lkp']) }}

SELECT
	TP_BROKER_GROUP_CAT_ID,
	UNID_PATY_CATG_C 
FROM {{ ref('XfmConversions') }}