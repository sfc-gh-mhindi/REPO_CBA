{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__appt__acqr__srce__pl__mrkt__srce__cat__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_APPT_ACQR_SRCELkp']) }}

SELECT
	PL_MRKT_SRCE_CAT_ID,
	ACQR_SRCE_C 
FROM {{ ref('XfmConversions') }}