{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__tpb__appt__qlfy__sbty__code', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_APPT_QLFYLkp']) }}

SELECT
	CHL_TPB_SUBTYPE_CODE,
	APPT_QLFY_C 
FROM {{ ref('XfmConversions__OutMAP_CSE_APPT_QLFYLks_CP') }}