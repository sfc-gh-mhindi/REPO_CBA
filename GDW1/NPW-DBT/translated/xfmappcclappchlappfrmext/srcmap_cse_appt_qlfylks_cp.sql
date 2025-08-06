{{ config(materialized='view', tags=['XfmAppCclAppChlAppFrmExt']) }}

WITH 
_cba__app_csel4_dev_lookupset_map__tpb__appt__qlfy__sbty__code AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_lookupset_map__tpb__appt__qlfy__sbty__code")  }})
SrcMAP_CSE_APPT_QLFYLks_CP AS (
	SELECT CHL_TPB_SUBTYPE_CODE,
		APPT_QLFY_C
	FROM _cba__app_csel4_dev_lookupset_map__tpb__appt__qlfy__sbty__code
)

SELECT * FROM SrcMAP_CSE_APPT_QLFYLks_CP